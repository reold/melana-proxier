import asyncio
import base64
import binascii
import hashlib
import ipaddress
import json
import os
import re
import socket
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from diskcache import Cache
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ValidationError

# ── Optional Cloudflare solver ─────────────────────────────────────────
try:
    from curl_cffi import requests as curl_requests

    CURL_CFFI_AVAILABLE = True
except Exception:
    curl_requests = None  # type: ignore
    CURL_CFFI_AVAILABLE = False

M3U8_CONTENT_TYPES = {
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
    "audio/x-mpegurl",
}

URI_ATTR_RE = re.compile(r'URI="([^"]+)"')

http_client: Optional[httpx.AsyncClient] = None

# Per-process single-flight map for playlist/cacheable non-stream fetches.
inflight_requests: dict[str, asyncio.Task] = {}
inflight_lock = asyncio.Lock()

# Per-process shared streaming map for media segments.
inflight_media: dict[str, "MediaFlight"] = {}
inflight_media_lock = asyncio.Lock()

MAX_REDIRECTS = 5

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
}

CF_PROBE_URL = os.getenv("CF_PROBE_URL", "").strip() or None

CF_HTML_INDICATORS = (
    "cf-browser-verification",
    "challenge-platform",
    "turnstile",
    "cf_chl",
    "jschl_vc",
    "just a moment",
    "managed challenge",
    "checking your browser",
    "cf-clearance",
)


class CFSolver:
    """
    Per-domain single-flight solver.
    Solves CF once per domain, harvests cookies into httpx, then gets out of the way.
    """

    def __init__(self):
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._domain_solved: set[str] = set()
        self._master = asyncio.Lock()

    def _host(self, url: str) -> str:
        return urlparse(url).hostname or url

    def is_challenge(self, response: httpx.Response) -> bool:
        """Detect Cloudflare challenge pages (buffered responses only)."""
        if response.status_code not in (403, 503, 429, 200):
            return False
        if "cloudflare" in response.headers.get("server", "").lower():
            if "text/html" in response.headers.get("content-type", ""):
                return True
        if "text/html" not in response.headers.get("content-type", ""):
            return False
        try:
            preview = response.text[:4096].lower()
        except Exception:
            return False
        return any(ind in preview for ind in CF_HTML_INDICATORS)

    async def solve(self, url: str, headers: dict) -> None:
        if not CURL_CFFI_AVAILABLE:
            return
        host = self._host(url)
        async with self._master:
            lock = self._domain_locks.get(host)
            if lock is None:
                lock = asyncio.Lock()
                self._domain_locks[host] = lock

        async with lock:
            if host in self._domain_solved:
                return

            curl_headers = {}
            for k in ("Referer", "Origin"):
                if headers.get(k):
                    curl_headers[k] = headers[k]

            try:
                resp = await run_in_threadpool(
                    curl_requests.get,
                    url,
                    headers=curl_headers or None,
                    impersonate="chrome124",
                    timeout=30,
                    allow_redirects=True,
                )
            except Exception as exc:
                print(f"[CF] curl_cffi solver error for {host}: {exc}")
                return

            if resp.status_code == 200:
                jar = getattr(resp, "cookies", None)
                if jar and http_client is not None:
                    for c in jar:
                        http_client.cookies.set(
                            c.name,
                            c.value,
                            domain=getattr(c, "domain", None) or host,
                            path=getattr(c, "path", "/") or "/",
                        )
                self._domain_solved.add(host)
                print(f"[CF] Cookies seeded for {host}")
            else:
                print(f"[CF] Solver returned {resp.status_code} for {host}")

    def invalidate(self, url: str) -> None:
        """Call when cached cookies expired and we hit another challenge."""
        self._domain_solved.discard(self._host(url))


cf_solver = CFSolver()


class ProxyData(BaseModel):
    url: str
    origin: Optional[str] = None
    referer: Optional[str] = None
    src: bool = False


class MediaFlight:
    def __init__(self):
        self.chunks: list[bytes] = []
        self.done = False
        self.error: Optional[BaseException] = None
        self.ready = asyncio.Event()
        self.condition = asyncio.Condition()
        self.status_code: int = 200
        self.content_type: str = "application/octet-stream"
        self.response_headers: dict = {}
        self.cacheable = False


def model_to_dict(model: BaseModel) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_none=True)
    return model.dict(exclude_none=True)


def is_absolute_url(url: str) -> bool:
    return url.startswith(("http://", "https://"))


def resolve_url(base_url: str, relative_url: str) -> str:
    try:
        if is_absolute_url(relative_url):
            return relative_url
        base = httpx.URL(base_url)
        return str(base.join(relative_url))
    except Exception:
        return urljoin(base_url, relative_url)


def is_ip_blocked(ip: ipaddress._BaseAddress) -> bool:
    if not ip.is_global:
        return True
    if ip == ipaddress.ip_address("169.254.169.254"):
        return True
    return False


def is_safe_url_syntax(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if not parsed.hostname:
            return False
        hostname = parsed.hostname.strip().lower().rstrip(".")
        if hostname in BLOCKED_HOSTNAMES:
            return False
        if hostname.endswith(".localhost"):
            return False
        if "." not in hostname and not re.match(r"^\[?[0-9a-f:.]+\]?$", hostname):
            return False
        try:
            ip = ipaddress.ip_address(hostname)
            if is_ip_blocked(ip):
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False


async def assert_safe_url(url: str) -> None:
    if not is_safe_url_syntax(url):
        raise HTTPException(status_code=403, detail="Unsafe or invalid URL requested.")
    parsed = urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(status_code=403, detail="Unsafe or invalid URL requested.")
    try:
        infos = await run_in_threadpool(
            socket.getaddrinfo,
            hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror:
        raise HTTPException(status_code=403, detail="Unable to resolve requested host.")

    resolved_ips = set()
    for info in infos:
        sockaddr = info[4]
        ip_text = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_text)
        except ValueError:
            raise HTTPException(status_code=403, detail="Invalid resolved IP.")
        resolved_ips.add(str(ip))
        if is_ip_blocked(ip):
            raise HTTPException(
                status_code=403,
                detail=f"Blocked unsafe resolved IP: {ip}",
            )
    if not resolved_ips:
        raise HTTPException(status_code=403, detail="Host resolved to no IPs.")


def determine_content_type(url: str, response_content_type: str) -> str:
    url_lower = url.lower()
    upstream_ct = (response_content_type or "").split(";", 1)[0].strip().lower()
    if ".m3u8" in url_lower:
        return "application/vnd.apple.mpegurl"
    if ".ts" in url_lower:
        return "video/mp2t"
    if ".mp4" in url_lower:
        return "video/mp4"
    if ".webvtt" in url_lower or ".vtt" in url_lower:
        return "text/vtt"
    if ".m4s" in url_lower:
        return "video/iso.segment"
    if upstream_ct:
        return upstream_ct
    return "application/octet-stream"


def build_cache_key(data: ProxyData) -> str:
    canonical = json.dumps(
        {
            "url": data.url,
            "origin": data.origin,
            "referer": data.referer,
            "src": data.src,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def encode_proxy_data(data: ProxyData) -> str:
    payload = json.dumps(
        model_to_dict(data),
        sort_keys=True,
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8").rstrip("=")


def build_proxied_url(
    absolute_url: str,
    original_data: ProxyData,
    server_origin: str,
) -> str:
    is_playlist = ".m3u8" in absolute_url.lower()
    new_proxy_data = ProxyData(
        url=absolute_url,
        origin=original_data.origin,
        referer=original_data.referer,
        src=is_playlist,
    )
    encoded = encode_proxy_data(new_proxy_data)
    proxied_url = f"{server_origin}/url/{encoded}"
    if is_playlist:
        proxied_url += ".m3u8"
    return proxied_url


def rewrite_uri_attributes(
    line: str,
    base_url: str,
    original_data: ProxyData,
    server_origin: str,
) -> str:
    def _replace(match: re.Match) -> str:
        raw_uri = match.group(1)
        absolute_url = resolve_url(base_url, raw_uri)
        proxied_url = build_proxied_url(absolute_url, original_data, server_origin)
        return f'URI="{proxied_url}"'

    return URI_ATTR_RE.sub(_replace, line)


def rewrite_m3u8_urls(
    content: str,
    original_data: ProxyData,
    server_origin: str,
) -> str:
    base_url = original_data.url
    lines = content.splitlines()
    rewritten_lines = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            rewritten_lines.append(line)
            continue

        if stripped.startswith("#"):
            rewritten_lines.append(
                rewrite_uri_attributes(line, base_url, original_data, server_origin)
            )
            continue

        try:
            absolute_url = resolve_url(base_url, stripped)
            proxied_url = build_proxied_url(absolute_url, original_data, server_origin)
            rewritten_lines.append(proxied_url)
        except Exception as e:
            print(f"Error rewriting URL '{stripped}': {e}")
            rewritten_lines.append(line)

    return "\n".join(rewritten_lines) + ("\n" if content.endswith("\n") else "")


def decode_proxy_data(base64_data: str) -> ProxyData:
    try:
        # Strip .m3u8 suffix first
        if base64_data.endswith(".m3u8"):
            base64_data = base64_data[:-5]

        # Strip trailing slashes/whitespace
        base64_data = base64_data.rstrip("/ \t\r\n")

        # Re-add base64 padding that was stripped on encode
        padding_needed = (4 - len(base64_data) % 4) % 4
        base64_data += "=" * padding_needed

        # validate=True already rejects any non-base64 characters with binascii.Error
        decoded_bytes = base64.b64decode(
            base64_data,
            altchars=b"-_",
            validate=True,
        )
        json_data = json.loads(decoded_bytes.decode("utf-8"))
        data = ProxyData(**json_data)

        if not is_safe_url_syntax(data.url):
            raise HTTPException(
                status_code=403,
                detail="Unsafe or invalid URL requested.",
            )

        return data

    except HTTPException:
        raise
    except (binascii.Error, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid base64 encoding: {e}")
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in proxy data: {e}")
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid proxy data format: {e}")


def get_server_origin(request: Request) -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL
    return f"{request.url.scheme}://{request.url.netloc}"


def is_probably_playlist(proxy_data: ProxyData) -> bool:
    return proxy_data.src or ".m3u8" in proxy_data.url.lower()


def build_upstream_headers(
    proxy_data: ProxyData,
    request: Optional[Request] = None,
) -> dict:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) "
            "Gecko/20100101 Firefox/126.0"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }

    if proxy_data.origin:
        headers["Origin"] = proxy_data.origin
    if proxy_data.referer:
        headers["Referer"] = proxy_data.referer
    elif proxy_data.origin:
        headers["Referer"] = proxy_data.origin.rstrip("/") + "/"

    if request is not None:
        for client_header, upstream_header in (
            ("range", "Range"),
            ("if-range", "If-Range"),
            ("if-none-match", "If-None-Match"),
            ("if-modified-since", "If-Modified-Since"),
        ):
            value = request.headers.get(client_header)
            if value:
                headers[upstream_header] = value

    return headers


async def safe_get(
    url: str,
    headers: dict,
    *,
    allow_cf_solve: bool = True,
) -> httpx.Response:
    if http_client is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")

    current_url = url
    current_headers = dict(headers)

    for _ in range(MAX_REDIRECTS + 1):
        await assert_safe_url(current_url)

        response = await http_client.get(
            current_url,
            headers=current_headers,
            follow_redirects=False,
        )

        if response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("location")
            if not location:
                return response
            current_headers["Referer"] = current_url
            current_url = resolve_url(current_url, location)
            await response.aclose()
            continue

        if allow_cf_solve and cf_solver.is_challenge(response):
            cf_solver.invalidate(current_url)
            await cf_solver.solve(current_url, current_headers)
            await response.aclose()
            return await safe_get(
                current_url,
                current_headers,
                allow_cf_solve=False,
            )

        return response

    raise HTTPException(status_code=508, detail="Too many upstream redirects.")


async def safe_stream_get(url: str, headers: dict) -> httpx.Response:
    if http_client is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")

    current_url = url
    current_headers = dict(headers)

    for _ in range(MAX_REDIRECTS + 1):
        await assert_safe_url(current_url)

        req = http_client.build_request(
            "GET",
            current_url,
            headers=current_headers,
        )
        response = await http_client.send(
            req,
            stream=True,
            follow_redirects=False,
        )

        if response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("location")
            if not location:
                return response
            await response.aclose()
            current_headers["Referer"] = current_url
            current_url = resolve_url(current_url, location)
            continue

        return response

    raise HTTPException(status_code=508, detail="Too many upstream redirects.")


def parse_single_range(range_header: str, total_size: int) -> Optional[tuple[int, int]]:
    if not range_header:
        return None
    match = re.match(r"^bytes=(\d*)-(\d*)$", range_header.strip())
    if not match:
        return None
    start_text, end_text = match.groups()
    if start_text == "" and end_text == "":
        return None
    if start_text == "":
        suffix_len = int(end_text)
        if suffix_len <= 0:
            return None
        start = max(total_size - suffix_len, 0)
        end = total_size - 1
        return start, end
    start = int(start_text)
    if end_text == "":
        end = total_size - 1
    else:
        end = int(end_text)
    if start >= total_size:
        return None
    if end >= total_size:
        end = total_size - 1
    if start > end:
        return None
    return start, end


def make_416_response(total_size: int) -> Response:
    return Response(
        status_code=416,
        headers={
            "Content-Range": f"bytes */{total_size}",
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "*",
        },
    )


def cached_range_response(
    cached_result: tuple,
    range_header: str,
) -> Optional[Response]:
    content, content_type, response_headers, status_code = cached_result
    if not isinstance(content, bytes):
        return None
    total_size = len(content)
    parsed_range = parse_single_range(range_header, total_size)
    if parsed_range is None:
        return make_416_response(total_size)
    start, end = parsed_range
    body = content[start : end + 1]
    headers = dict(response_headers)
    headers.pop("Content-Length", None)
    headers.pop("Content-Range", None)
    headers.update(
        {
            "Content-Range": f"bytes {start}-{end}/{total_size}",
            "Content-Length": str(len(body)),
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "*",
        }
    )
    return Response(
        content=body,
        status_code=206,
        media_type=content_type,
        headers=headers,
    )


def is_cacheable_full_media_response(status_code: int, request: Request) -> bool:
    if request.headers.get("range"):
        return False
    return status_code == 200


def build_downstream_media_headers(upstream: httpx.Response) -> dict:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "*",
        "Cache-Control": "public, max-age=3600, immutable",
    }
    for header in (
        "Last-Modified",
        "ETag",
        "Accept-Ranges",
        "Content-Length",
        "Content-Range",
    ):
        if header in upstream.headers:
            headers[header] = upstream.headers[header]
    return headers


async def fetch_upstream_result(proxy_data: ProxyData, request: Request):
    request_headers = build_upstream_headers(proxy_data, request=None)
    try:
        upstream = await safe_get(proxy_data.url, headers=request_headers)
        upstream.raise_for_status()
    except httpx.HTTPStatusError as e:
        if cf_solver.is_challenge(e.response):
            raise HTTPException(
                status_code=403,
                detail="Upstream Cloudflare challenge blocked the request and curl_cffi is not available or failed.",
            )
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Upstream returned {e.response.status_code}: {e.response.text[:200]}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Upstream request timed out")
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Upstream request failed: {e}",
        )

    content_type = determine_content_type(
        proxy_data.url,
        upstream.headers.get("content-type", ""),
    )

    is_m3u8 = ".m3u8" in proxy_data.url.lower() or content_type in M3U8_CONTENT_TYPES
    will_modify = proxy_data.src and is_m3u8
    is_live_playlist = False

    response_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "*",
    }

    if is_m3u8:
        content = upstream.text
        if will_modify:
            content = rewrite_m3u8_urls(
                content,
                proxy_data,
                get_server_origin(request),
            )
        if "#EXT-X-ENDLIST" not in content:
            is_live_playlist = True
            response_headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        else:
            response_headers["Cache-Control"] = "public, max-age=3600"
    else:
        content = upstream.content
        response_headers["Cache-Control"] = "public, max-age=3600, immutable"

    if not will_modify:
        for header in (
            "Last-Modified",
            "ETag",
            "Accept-Ranges",
            "Content-Length",
        ):
            if header in upstream.headers:
                response_headers[header] = upstream.headers[header]

    result = (
        content,
        content_type,
        response_headers,
        upstream.status_code,
    )
    return result, is_live_playlist


async def fetch_cache_and_store(
    cache_key: str,
    proxy_data: ProxyData,
    request: Request,
):
    result, is_live_playlist = await fetch_upstream_result(proxy_data, request)
    if not is_live_playlist:
        await run_in_threadpool(cache.set, cache_key, result)
    return result


async def get_or_build_response(
    cache_key: str,
    proxy_data: ProxyData,
    request: Request,
):
    cached_value = await run_in_threadpool(cache.get, cache_key)
    if cached_value is not None:
        return cached_value

    async with inflight_lock:
        cached_value = await run_in_threadpool(cache.get, cache_key)
        if cached_value is not None:
            return cached_value

        task = inflight_requests.get(cache_key)
        if task is None:
            task = asyncio.create_task(
                fetch_cache_and_store(cache_key, proxy_data, request)
            )
            inflight_requests[cache_key] = task

    try:
        return await task
    finally:
        if task.done():
            async with inflight_lock:
                if inflight_requests.get(cache_key) is task:
                    inflight_requests.pop(cache_key, None)


async def produce_media_flight(
    cache_key: str,
    proxy_data: ProxyData,
    first_request: Request,
    flight: MediaFlight,
):
    upstream: Optional[httpx.Response] = None
    try:
        request_headers = build_upstream_headers(proxy_data, first_request)
        upstream = await safe_stream_get(proxy_data.url, request_headers)

        if upstream.status_code >= 400:
            body = await upstream.aread()
            raise HTTPException(
                status_code=upstream.status_code,
                detail=f"Upstream returned {upstream.status_code}: {body[:200]!r}",
            )

        flight.status_code = upstream.status_code
        flight.content_type = determine_content_type(
            proxy_data.url,
            upstream.headers.get("content-type", ""),
        )
        flight.response_headers = build_downstream_media_headers(upstream)
        flight.cacheable = is_cacheable_full_media_response(
            upstream.status_code,
            first_request,
        )
        flight.ready.set()

        async for chunk in upstream.aiter_bytes(64 * 1024):
            if not chunk:
                continue
            async with flight.condition:
                flight.chunks.append(chunk)
                flight.condition.notify_all()

        if flight.cacheable:
            body = b"".join(flight.chunks)
            cached_headers = dict(flight.response_headers)
            cached_headers["Content-Length"] = str(len(body))
            cached_headers["Accept-Ranges"] = "bytes"
            cached_result = (
                body,
                flight.content_type,
                cached_headers,
                flight.status_code,
            )
            await run_in_threadpool(cache.set, cache_key, cached_result)

    except BaseException as e:
        flight.error = e
        flight.ready.set()
    finally:
        if upstream is not None:
            await upstream.aclose()
        async with flight.condition:
            flight.done = True
            flight.condition.notify_all()
        async with inflight_media_lock:
            if inflight_media.get(cache_key) is flight:
                inflight_media.pop(cache_key, None)


async def media_flight_body(flight: MediaFlight):
    index = 0
    while True:
        async with flight.condition:
            await flight.condition.wait_for(
                lambda: index < len(flight.chunks) or flight.done or flight.error
            )
        if index < len(flight.chunks):
            chunk = flight.chunks[index]
            index += 1
        elif flight.error:
            raise flight.error
        elif flight.done:
            break
        else:
            continue
        yield chunk


async def stream_media_with_shared_cache(
    cache_key: str,
    proxy_data: ProxyData,
    request: Request,
):
    cached_value = await run_in_threadpool(cache.get, cache_key)
    if cached_value is not None:
        content, content_type, response_headers, status_code = cached_value
        return Response(
            content=content,
            status_code=status_code,
            media_type=content_type,
            headers=response_headers,
        )

    async with inflight_media_lock:
        flight = inflight_media.get(cache_key)
        if flight is None:
            flight = MediaFlight()
            inflight_media[cache_key] = flight
            asyncio.create_task(
                produce_media_flight(
                    cache_key,
                    proxy_data,
                    request,
                    flight,
                )
            )

    await flight.ready.wait()

    if flight.error is not None and not flight.chunks:
        if isinstance(flight.error, HTTPException):
            raise flight.error
        raise HTTPException(status_code=502, detail=str(flight.error))

    return StreamingResponse(
        media_flight_body(flight),
        status_code=flight.status_code,
        media_type=flight.content_type,
        headers=flight.response_headers,
    )


async def stream_range_no_cache(proxy_data: ProxyData, request: Request):
    request_headers = build_upstream_headers(proxy_data, request)
    upstream = await safe_stream_get(proxy_data.url, request_headers)

    if upstream.status_code >= 400:
        body = await upstream.aread()
        await upstream.aclose()
        raise HTTPException(
            status_code=upstream.status_code,
            detail=f"Upstream returned {upstream.status_code}: {body[:200]!r}",
        )

    content_type = determine_content_type(
        proxy_data.url,
        upstream.headers.get("content-type", ""),
    )
    response_headers = build_downstream_media_headers(upstream)

    async def body():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
                yield chunk
        finally:
            await upstream.aclose()

    return StreamingResponse(
        body(),
        status_code=upstream.status_code,
        media_type=content_type,
        headers=response_headers,
    )


# ── App setup ──────────────────────────────────────────────────────────

cache_size_gb = float(os.getenv("CACHE_SIZE", "0.4"))
cache_size_bytes = int(cache_size_gb * 1024 * 1024 * 1024)
cache_dir = os.getenv("CACHE_DIR", "/tmp/m3u8_cache")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        http2=True,
        follow_redirects=False,
        timeout=30.0,
        limits=httpx.Limits(
            max_connections=500,
            max_keepalive_connections=100,
        ),
    )

    if CURL_CFFI_AVAILABLE and CF_PROBE_URL:
        try:
            probe_data = ProxyData(url=CF_PROBE_URL)
            await cf_solver.solve(CF_PROBE_URL, build_upstream_headers(probe_data))
        except Exception as exc:
            print(f"[CF] Startup probe failed: {exc}")

    yield

    await http_client.aclose()


app = FastAPI(
    title="M3U8 Proxy Server",
    description="A FastAPI server to proxy and rewrite M3U8 playlists.",
    version="2.3.1",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

cache = Cache(
    cache_dir,
    size_limit=cache_size_bytes,
    eviction_policy="least-recently-used",
)


@app.get("/url/{base64_data:path}")
async def m3u8_proxy(base64_data: str, request: Request):
    proxy_data = decode_proxy_data(base64_data)
    await assert_safe_url(proxy_data.url)
    cache_key = build_cache_key(proxy_data)

    range_header = request.headers.get("range")
    if range_header:
        cached_value = await run_in_threadpool(cache.get, cache_key)
        if cached_value is not None:
            range_response = cached_range_response(cached_value, range_header)
            if range_response is not None:
                return range_response
        return await stream_range_no_cache(proxy_data, request)

    if is_probably_playlist(proxy_data):
        content, content_type, response_headers, status_code = (
            await get_or_build_response(
                cache_key,
                proxy_data,
                request,
            )
        )
        return Response(
            content=content,
            status_code=status_code,
            media_type=content_type,
            headers=response_headers,
        )

    return await stream_media_with_shared_cache(
        cache_key,
        proxy_data,
        request,
    )


@app.get("/")
def read_root():
    try:
        num_entries = len(cache)
        current_bytes = cache.volume()
    except Exception:
        num_entries = -1
        current_bytes = -1

    size_limit = cache.size_limit
    if size_limit > 0 and current_bytes >= 0:
        utilization = round((current_bytes / size_limit) * 100, 2)
    else:
        utilization = None

    return {
        "status": "online",
        "service": "M3U8 Proxy Server",
        "version": "2.3.1",
        "endpoint": "/url/<base64_data>",
        "public_base_url": PUBLIC_BASE_URL or None,
        "cache": {
            "entries": num_entries,
            "current_bytes": current_bytes,
            "max_bytes": size_limit,
            "max_gb": cache_size_gb,
            "utilization_percent": utilization,
        },
    }
