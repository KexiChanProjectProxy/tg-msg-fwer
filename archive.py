"""Archive detection, extraction, and media classification utilities."""

import asyncio
import logging
import shutil
import zipfile
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
_VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm"}
_ARCHIVE_EXTS = {".zip", ".rar", ".7z"}
_ARCHIVE_MIMES = {
    "application/zip",
    "application/x-rar-compressed",
    "application/x-7z-compressed",
    "application/x-zip-compressed",
}


def is_archive(message) -> bool:
    """Return True if the message is a supported compressed archive document."""
    doc = getattr(getattr(message, "media", None), "document", None)
    if doc is None:
        return False

    mime = getattr(doc, "mime_type", "") or ""
    if mime in _ARCHIVE_MIMES:
        return True

    for attr in getattr(doc, "attributes", []):
        fname = getattr(attr, "file_name", None)
        if fname and Path(fname).suffix.lower() in _ARCHIVE_EXTS:
            return True

    return False


async def extract_archive(path: Path, dest: Path) -> Path:
    """
    Extract *path* into *dest*.

    Supports .zip natively; delegates .rar and .7z to CLI tools.
    Returns *dest* (already exists after extraction).
    """
    dest.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()

    if suffix == ".zip":
        await asyncio.get_event_loop().run_in_executor(
            None, _extract_zip, path, dest
        )
    elif suffix == ".rar":
        await _run_cmd(["unrar", "x", "-y", str(path), str(dest) + "/"])
    elif suffix == ".7z":
        await _run_cmd(["7z", "x", str(path), f"-o{dest}", "-y"])
    else:
        raise ValueError(f"Unsupported archive format: {suffix}")

    return dest


def classify_media(dest: Path) -> Tuple[List[Path], List[Path]]:
    """
    Walk *dest* recursively and classify files by extension.

    Returns ``(images, videos)`` — each a list of Path objects.
    Files with unknown extensions are ignored.
    """
    images: List[Path] = []
    videos: List[Path] = []

    for p in sorted(dest.rglob("*")):
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext in _IMAGE_EXTS:
            images.append(p)
        elif ext in _VIDEO_EXTS:
            videos.append(p)

    return images, videos


# ── internal helpers ─────────────────────────────────────────────────────────

def _extract_zip(path: Path, dest: Path) -> None:
    with zipfile.ZipFile(path, "r") as zf:
        zf.extractall(dest)


async def _run_cmd(cmd: List[str]) -> None:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command {cmd[0]} failed (exit {proc.returncode}): "
            f"{stderr.decode(errors='replace').strip()}"
        )
