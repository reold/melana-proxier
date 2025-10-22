import base64
import json
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError
from diskcache import Cache

app = FastAPI(
    title="M3U8 Proxy Server",
    description="A FastAPI server to proxy and rewrite M3U8 playlists.",
    version="2.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create a disk cache with LRU eviction and 1GB size limit
cache = Cache(
    # for disk based cache
    # "/tmp/m3u8_cache",
    size_limit=400 * 1024 * 1024,  # 400 MB
    eviction_policy="least-recently-used",
    memory=True,
)


class ProxyData(BaseModel):
    url: str
    origin: Optional[str] = None
    referer: Optional[str] = None
    src: bool = False


def is_absolute_url(url: str) -> bool:
    """Check if URL is absolute"""
    return url.startswith(("http://", "https://"))


def is_likely_media_url(line: str) -> bool:
    """
    Determine if a line is likely a media URL or segment.
    Checks for common media file extensions and URL patterns.
    """
    if not line or line.startswith("#"):
        return False

    # Check for absolute or relative URLs
    if line.startswith(("http://", "https://", "/")):
        return True

    # Check for common media file extensions
    media_extensions = r"\.(ts|m3u8|m4s|mp4|key|aac|mp3|vtt|webvtt)(\?.*)?$"
    return re.search(media_extensions, line, re.IGNORECASE) is not None


def resolve_url(base_url: str, relative_url: str) -> str:
    """
    Safely resolve a relative URL against a base URL.
    Handles edge cases and malformed URLs.
    """
    try:
        # If already absolute, return as-is
        if is_absolute_url(relative_url):
            return relative_url

        # Use httpx.URL for robust URL handling
        base = httpx.URL(base_url)
        resolved = base.join(relative_url)
        return str(resolved)
    except Exception:
        # Fallback to urljoin
        return urljoin(base_url, relative_url)


def rewrite_m3u8_urls(content: str, original_data: ProxyData, current_url: str) -> str:
    """
    Rewrites URLs inside an M3U8 playlist to point back to this proxy.
    Handles both master playlists and media playlists.
    """
    try:
        parsed_current = urlparse(current_url)
        server_origin = f"{parsed_current.scheme}://{parsed_current.netloc}"
    except Exception:
        # Fallback
        server_origin = "/".join(str(current_url).split("/")[:3])

    base_url = original_data.url
    lines = content.split("\n")
    rewritten_lines = []

    for line in lines:
        stripped = line.strip()

        # Pass through comments and empty lines
        if not stripped or stripped.startswith("#"):
            rewritten_lines.append(line)
            continue

        # Check if this line contains a URL
        if is_likely_media_url(stripped):
            try:
                # Resolve to absolute URL
                absolute_url = resolve_url(base_url, stripped)

                # Create new proxy configuration
                new_proxy_data = ProxyData(
                    url=absolute_url,
                    origin=original_data.origin,
                    referer=original_data.referer,
                    src=False,  # Segments should not be rewritten further
                )

                # Encode the proxy data
                json_payload = json.dumps(
                    new_proxy_data.dict(exclude_none=True), separators=(",", ":")
                )
                base64_encoded = (
                    base64.urlsafe_b64encode(json_payload.encode("utf-8"))
                    .decode("utf-8")
                    .rstrip("=")
                )

                # Determine if this is a playlist (for extension hint)
                is_playlist = ".m3u8" in absolute_url.lower()

                # Build proxied URL
                proxied_url = f"{server_origin}/url/{base64_encoded}"
                if is_playlist:
                    proxied_url += ".m3u8"

                rewritten_lines.append(proxied_url)
            except Exception as e:
                # Log error but don't break the entire playlist
                print(f"Error rewriting URL '{stripped}': {e}")
                rewritten_lines.append(line)
        else:
            # Not a URL, pass through unchanged
            rewritten_lines.append(line)

    return "\n".join(rewritten_lines)


def decode_proxy_data(base64_data: str) -> ProxyData:
    """
    Decode and validate base64-encoded proxy data.
    Raises HTTPException on invalid data.
    """
    try:
        # Remove optional .m3u8 extension
        if base64_data.endswith(".m3u8"):
            base64_data = base64_data[:-5]

        # Add padding if necessary
        padding_needed = (4 - len(base64_data) % 4) % 4
        base64_data += "=" * padding_needed

        # Decode base64
        decoded_bytes = base64.urlsafe_b64decode(base64_data)
        json_data = json.loads(decoded_bytes.decode("utf-8"))

        # Validate with Pydantic
        return ProxyData(**json_data)

    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in proxy data: {e}")
    except base64.binascii.Error as e:
        raise HTTPException(status_code=400, detail=f"Invalid base64 encoding: {e}")
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid proxy data format: {e}")


def determine_content_type(url: str, response_content_type: str) -> str:
    """
    Determine the appropriate content type based on URL and upstream response.
    """
    url_lower = url.lower()

    # Check URL extension first
    if ".m3u8" in url_lower:
        return "application/vnd.apple.mpegurl"
    elif ".ts" in url_lower:
        return "video/mp2t"
    elif ".mp4" in url_lower:
        return "video/mp4"
    elif ".webvtt" in url_lower or ".vtt" in url_lower:
        return "text/vtt"
    elif ".m4s" in url_lower:
        return "video/iso.segment"

    # Fall back to upstream content type
    return response_content_type or "application/octet-stream"


@app.get("/url/{base64_data:path}")
async def m3u8_proxy(base64_data: str, request: Request):
    # Decode and validate proxy data
    proxy_data = decode_proxy_data(base64_data)

    cache_key = base64_data  # Use base64_data as unique cache key

    # Check cache first
    cached_value = cache.get(cache_key)
    if cached_value is not None:
        content, content_type, headers = cached_value
        return Response(content=content, media_type=content_type, headers=headers)

    # Build headers for upstream request
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }

    if proxy_data.origin:
        headers["Origin"] = proxy_data.origin
    if proxy_data.referer:
        headers["Referer"] = proxy_data.referer

    async with httpx.AsyncClient(
        http2=True, follow_redirects=True, timeout=30.0
    ) as client:
        try:
            response = await client.get(proxy_data.url, headers=headers)
            response.raise_for_status()
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

    # Determine content type
    content_type = determine_content_type(
        proxy_data.url, response.headers.get("content-type", "")
    )

    is_m3u8 = (
        ".m3u8" in proxy_data.url.lower()
        or content_type == "application/vnd.apple.mpegurl"
    )
    will_modify = proxy_data.src and is_m3u8

    if will_modify:
        content = response.text
        content = rewrite_m3u8_urls(content, proxy_data, str(request.url))
        content_bytes = content.encode("utf-8")
    else:
        content_bytes = response.content

    response_headers = {
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "*",
    }

    if not will_modify:
        for header in ["Content-Length", "Last-Modified", "ETag"]:
            if header in response.headers:
                response_headers[header] = response.headers[header]

    # Cache the response content, content type, and headers as a tuple
    cache.set(cache_key, (content_bytes, content_type, response_headers))

    return Response(
        content=content_bytes, media_type=content_type, headers=response_headers
    )


# Health endpoints unchanged
@app.get("/")
def read_root():
    return {
        "status": "online",
        "service": "M3U8 Proxy Server",
        "version": "2.0.0",
        "endpoint": "/url/<base64_data>",
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}
