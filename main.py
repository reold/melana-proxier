import asyncio
import base64
import binascii
import hashlib
import json
import os
import re
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from diskcache import Cache
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError

M3U8_CONTENT_TYPES = {
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
    "audio/x-mpegurl",
}

URI_ATTR_RE = re.compile(r'URI="([^"]+)"')

http_client: Optional[httpx.AsyncClient] = None

# Per-process single-flight map:
# one upstream fetch per cache key at a time
inflight_requests: dict[str, asyncio.Task] = {}
inflight_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client

    http_client = httpx.AsyncClient(
        http2=True,
        follow_redirects=True,
        timeout=30.0,
        limits=httpx.Limits(
            max_connections=500,
            max_keepalive_connections=100,
        ),
    )

    yield

    await http_client.aclose()


app = FastAPI(
    title="M3U8 Proxy Server",
    description="A FastAPI server to proxy and rewrite M3U8 playlists.",
    version="2.1.0",
    lifespan=lifespan,
)

cache_size_gb = float(os.getenv("CACHE_SIZE", "0.4"))
cache_size_bytes = int(cache_size_gb * 1024 * 1024 * 1024)
cache_dir = os.getenv("CACHE_DIR", "/tmp/m3u8_cache")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cache = Cache(
    cache_dir,
    size_limit=cache_size_bytes,
    eviction_policy="least-recently-used",
)


class ProxyData(BaseModel):
    url: str
    origin: Optional[str] = None
    referer: Optional[str] = None
    src: bool = False


def model_to_dict(model: BaseModel) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_none=True)
    return model.dict(exclude_none=True)


def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


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
    absolute_url: str, original_data: ProxyData, server_origin: str
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
    line: str, base_url: str, original_data: ProxyData, server_origin: str
) -> str:
    def _replace(match: re.Match) -> str:
        raw_uri = match.group(1)
        absolute_url = resolve_url(base_url, raw_uri)
        proxied_url = build_proxied_url(absolute_url, original_data, server_origin)
        return f'URI="{proxied_url}"'

    return URI_ATTR_RE.sub(_replace, line)


def rewrite_m3u8_urls(content: str, original_data: ProxyData, current_url: str) -> str:
    try:
        parsed_current = urlparse(current_url)
        server_origin = f"{parsed_current.scheme}://{parsed_current.netloc}"
    except Exception:
        server_origin = "/".join(str(current_url).split("/")[:3])

    base_url = original_data.url
    lines = content.splitlines()
    rewritten_lines = []

    for line in lines:
        stripped = line.strip()

        if not stripped:
            rewritten_lines.append(line)
            continue

        # Rewrite URI="..." attributes in playlist tags like:
        # #EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA, #EXT-X-I-FRAME-STREAM-INF, etc.
        if stripped.startswith("#"):
            rewritten_lines.append(
                rewrite_uri_attributes(line, base_url, original_data, server_origin)
            )
            continue

        # In HLS playlists, non-comment lines are URI lines
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
        if base64_data.endswith(".m3u8"):
            base64_data = base64_data[:-5]

        padding_needed = (4 - len(base64_data) % 4) % 4
        base64_data += "=" * padding_needed

        decoded_bytes = base64.urlsafe_b64decode(base64_data)
        json_data = json.loads(decoded_bytes.decode("utf-8"))

        data = ProxyData(**json_data)

        if not is_safe_url(data.url):
            raise HTTPException(
                status_code=403, detail="Unsafe or invalid URL requested."
            )

        return data

    except HTTPException:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in proxy data: {e}")
    except binascii.Error as e:
        raise HTTPException(status_code=400, detail=f"Invalid base64 encoding: {e}")
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid proxy data format: {e}")


async def fetch_upstream_result(proxy_data: ProxyData, request: Request):
    if http_client is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")

    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) "
            "Gecko/20100101 Firefox/143.0"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }

    if proxy_data.origin:
        request_headers["Origin"] = proxy_data.origin
    if proxy_data.referer:
        request_headers["Referer"] = proxy_data.referer

    try:
        upstream = await http_client.get(proxy_data.url, headers=request_headers)
        upstream.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Upstream returned {e.response.status_code}: {e.response.text[:200]}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Upstream request timed out")
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Upstream request failed: {str(e)}"
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
            content = rewrite_m3u8_urls(content, proxy_data, str(request.url))

        # No ENDLIST usually means live/event playlist
        if "#EXT-X-ENDLIST" not in content:
            is_live_playlist = True
            response_headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        else:
            response_headers["Cache-Control"] = "public, max-age=3600"

    else:
        content = upstream.content
        response_headers["Cache-Control"] = "public, max-age=3600, immutable"

    # Preserve safe upstream headers only when content was not modified
    if not will_modify:
        for header in ("Last-Modified", "ETag", "Accept-Ranges"):
            if header in upstream.headers:
                response_headers[header] = upstream.headers[header]

    result = (content, content_type, response_headers)
    return result, is_live_playlist


async def fetch_cache_and_store(
    cache_key: str, proxy_data: ProxyData, request: Request
):
    result, is_live_playlist = await fetch_upstream_result(proxy_data, request)

    # Do not cache live playlists
    if not is_live_playlist:
        await run_in_threadpool(cache.set, cache_key, result)

    return result


async def get_or_build_response(
    cache_key: str, proxy_data: ProxyData, request: Request
):
    # Fast path
    cached_value = await run_in_threadpool(cache.get, cache_key)
    if cached_value is not None:
        return cached_value

    # Slow path: coalesce concurrent cache misses
    async with inflight_lock:
        # Double-check after taking the lock
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


@app.get("/url/{base64_data:path}")
async def m3u8_proxy(base64_data: str, request: Request):
    proxy_data = decode_proxy_data(base64_data)
    cache_key = build_cache_key(proxy_data)

    content, content_type, response_headers = await get_or_build_response(
        cache_key, proxy_data, request
    )

    return Response(
        content=content,
        media_type=content_type,
        headers=response_headers,
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
        "version": "2.1.0",
        "endpoint": "/url/<base64_data>",
        "cache": {
            "entries": num_entries,
            "current_bytes": current_bytes,
            "max_bytes": size_limit,
            "max_gb": cache_size_gb,
            "utilization_percent": utilization,
        },
    }
