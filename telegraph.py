import asyncio
import json
import logging
import re
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import List, Tuple

import config

logger = logging.getLogger(__name__)

# Matches telegra.ph URLs, stops at whitespace, closing paren/bracket/quote
_TELEGRAPH_RE = re.compile(r'https?://telegra\.ph/[^\s\)\]\"\'>]+')


def find_telegraph_urls(text: str) -> List[str]:
    """Return all unique telegra.ph URLs found in text, in order of appearance."""
    seen = set()
    result = []
    for url in _TELEGRAPH_RE.findall(text or ""):
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


async def fetch_telegraph_page(url: str) -> Tuple[str, str, List[str]]:
    """
    Fetch a Telegraph page via the public API.

    Returns ``(title, body_text, image_urls)``.
    ``image_urls`` are fully qualified https://telegra.ph/file/... URLs.
    """
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.lstrip("/")
    api_url = f"https://api.telegra.ph/getPage/{path}?return_content=true"

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _fetch_json, api_url)

    if not data.get("ok"):
        raise ValueError(f"Telegraph API error: {data.get('error', 'unknown')}")

    result = data["result"]
    title = result.get("title", "")
    content = result.get("content", [])

    text_parts: List[str] = []
    image_urls: List[str] = []
    _parse_nodes(content, text_parts, image_urls)

    body = "\n".join(text_parts).strip()
    return title, body, image_urls


async def download_telegraph_images(image_urls: List[str]) -> List[Path]:
    if not image_urls:
        return []
    config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, _download_image, u) for u in image_urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    files = []
    for url, res in zip(image_urls, results):
        if isinstance(res, Exception):
            logger.warning(f"Failed to download Telegraph image {url}: {res}")
        else:
            files.append(res)
    return files


# ── internal helpers ─────────────────────────────────────────────────────────

def _fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "TelegramReload/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _download_image(url: str) -> Path:
    req = urllib.request.Request(url, headers={"User-Agent": "TelegramReload/1.0"})
    filename = urllib.parse.urlparse(url).path.split("/")[-1]
    suffix = Path(filename).suffix if filename else ""
    if not suffix:
        suffix = ".bin"
    out_path = config.TEMP_DIR / f"telegraph_{uuid.uuid4().hex}{suffix}"

    try:
        with urllib.request.urlopen(req, timeout=60) as resp, open(out_path, "wb") as out_file:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                out_file.write(chunk)
        return out_path
    except Exception:
        out_path.unlink(missing_ok=True)
        raise


def _parse_nodes(nodes, text_parts: List[str], image_urls: List[str]) -> None:
    """Recursively walk Telegraph content nodes, collecting text and image URLs."""
    for node in nodes:
        if isinstance(node, str):
            text_parts.append(node)
            continue

        if not isinstance(node, dict):
            continue

        tag = node.get("tag", "")
        attrs = node.get("attrs", {})
        children = node.get("children", [])

        if tag == "img":
            src = attrs.get("src", "")
            if src:
                if src.startswith("/"):
                    src = f"https://telegra.ph{src}"
                image_urls.append(src)

        elif tag == "br":
            text_parts.append("\n")

        elif tag in ("p", "blockquote"):
            _parse_nodes(children, text_parts, image_urls)
            text_parts.append("\n")

        elif tag == "figcaption":
            # Collect caption text inline, skip images inside captions
            cap_parts: List[str] = []
            _parse_nodes(children, cap_parts, [])
            cap = "".join(cap_parts).strip()
            if cap:
                text_parts.append(f"_{cap}_\n")

        else:
            # figure, aside, h3, h4, a, b, i, u, s, etc.
            _parse_nodes(children, text_parts, image_urls)
