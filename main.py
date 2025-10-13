import base64
import json
import re
from urllib.parse import urljoin, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="M3U8 Proxy Server",
    description="A FastAPI server to proxy and rewrite M3U8 playlists.",
    version="1.0.0",
)

# Configure CORS to allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def rewrite_m3u8_urls(content: str, original_data: dict, current_url: str) -> str:
    """
    Rewrites URLs inside an M3U8 playlist to point back to this proxy.
    """
    try:
        # Use httpx.URL for robust parsing and joining
        base_url = httpx.URL(original_data['url'])
        server_origin = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
    except Exception:
        # Fallback if URL parsing fails
        base_url_str = original_data['url']
        server_origin = "/".join(str(current_url).split("/")[:3])


    lines = content.split('\n')
    rewritten_lines = []

    for line in lines:
        trimmed_line = line.strip()

        if not trimmed_line or trimmed_line.startswith('#'):
            rewritten_lines.append(line)
            continue

        # Basic check to see if the line is a URL
        # M3U8 segments are often just filenames
        could_be_url = (
            trimmed_line.startswith(('http://', 'https://', '/')) or
            re.search(r'\.(ts|m3u8|m4s|mp4|key|aac|mp3)', trimmed_line, re.IGNORECASE) is not None
        )

        if could_be_url:
            try:
                # Resolve the relative URL to an absolute one
                target_url = urljoin(str(base_url), trimmed_line)

                new_proxy_data = {
                    "url": target_url,
                    "origin": original_data.get("origin"),
                    "referer": original_data.get("referer"),
                    "src": False,  # Always false for segments
                }
                
                json_payload = json.dumps(new_proxy_data, separators=(",", ":"))
                base64_encoded = base64.urlsafe_b64encode(json_payload.encode('utf-8')).decode('utf-8').rstrip("=")

                is_m3u8 = '.m3u8' in target_url.lower() or '.m3u8' in trimmed_line.lower()
                
                # Append .m3u8 for compatibility if it's a playlist
                final_proxied_url = f"{server_origin}/url/{base64_encoded}"
                if is_m3u8:
                    final_proxied_url += ".m3u8"
                
                rewritten_lines.append(final_proxied_url)
            except Exception as e:
                print(f"Error rewriting URL '{trimmed_line}': {e}")
                rewritten_lines.append(line)
        else:
            rewritten_lines.append(line)

    return '\n'.join(rewritten_lines)


@app.get("/url/{base64_data:path}")
async def m3u8_proxy(base64_data: str, request: Request):
    """
    The main proxy endpoint. Decodes the base64 URL, fetches the content,
    and rewrites it if necessary.
    """
    # Remove the optional .m3u8 extension
    if base64_data.endswith('.m3u8'):
        base64_data = base64_data[:-5]

    try:
        # Pad the base64 string to be a multiple of 4
        padding_needed = -len(base64_data) % 4
        base64_data += "=" * padding_needed
        
        decoded_bytes = base64.urlsafe_b64decode(base64_data)
        proxy_data = json.loads(decoded_bytes.decode('utf-8'))
    except (json.JSONDecodeError, base64.binascii.Error, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid base64 data: {e}")

    target_url = proxy_data.get("url")
    if not target_url:
        raise HTTPException(status_code=400, detail="Missing 'url' in proxy data")

    # Use the same headers that worked for you in Google Colab
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
    }
    if proxy_data.get("origin"):
        headers["Origin"] = proxy_data["origin"]
    if proxy_data.get("referer"):
        headers["Referer"] = proxy_data["referer"]

    async with httpx.AsyncClient(http2=True, follow_redirects=True, timeout=30.0) as client:
        try:
            res = await client.get(target_url, headers=headers)
            res.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"Failed to fetch upstream content: {e}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")

    content = res.text
    if proxy_data.get("src"):
        content = rewrite_m3u8_urls(content, proxy_data, str(request.url))

    # Determine the correct content type
    content_type = res.headers.get("content-type", "application/octet-stream")
    if "m3u8" in target_url:
        content_type = "application/vnd.apple.mpegurl"
    elif ".ts" in target_url:
        content_type = "video/mp2t"
    
    return Response(
        content=content,
        media_type=content_type,
        headers={"Cache-Control": "public, max-age=60"}
    )

@app.get("/")
def read_root():
    return {"status": "M3U8 Proxy Server is running. Use the /url/<base64_data> endpoint."}

# To run this app locally, use: uvicorn main:app --reload
