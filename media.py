"""Media type detection and ffmpeg conversion utilities for Telegram transfers."""

import asyncio
import io
import json
import logging
import os
import subprocess
import tempfile

from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeAnimated,
    DocumentAttributeSticker,
)

logger = logging.getLogger(__name__)


def get_media_type(message) -> str:
    """
    Return a string describing the media type of a message.

    Returns one of: "photo", "video", "voice", "animation", "video_note",
    "sticker", "document", or "text".
    """
    media = message.media
    if media is None:
        return "text"

    if isinstance(media, MessageMediaPhoto):
        return "photo"

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc is None:
            return "document"

        attrs = {type(a): a for a in (doc.attributes or [])}

        # Sticker (must check before animated/video)
        if DocumentAttributeSticker in attrs:
            return "sticker"

        # Animated (GIF converted to MP4 by Telegram)
        if DocumentAttributeAnimated in attrs:
            return "animation"

        # Audio: check voice flag
        if DocumentAttributeAudio in attrs:
            audio_attr = attrs[DocumentAttributeAudio]
            if getattr(audio_attr, "voice", False):
                return "voice"
            return "document"

        # Video: check video_note (round video) flag
        if DocumentAttributeVideo in attrs:
            video_attr = attrs[DocumentAttributeVideo]
            if getattr(video_attr, "round_message", False):
                return "video_note"
            return "video"

        return "document"

    return "document"


def guess_extension_from_bytes(buf: io.BytesIO) -> str:
    """
    Detect the file format of *buf* using magic bytes and return a file extension.

    The buffer position is restored to 0 after reading.
    Returns a bare extension string without a dot, e.g. ``"jpg"``, ``"mp4"``.
    Falls back to ``"bin"`` if no signature matches.
    """
    header = buf.read(12)
    buf.seek(0)

    if header[:3] == b'\xff\xd8\xff':
        return "jpg"
    if header[:8] == b'\x89PNG\r\n\x1a\n':
        return "png"
    if header[:4] == b'GIF8':
        return "gif"
    if header[:4] == b'RIFF' and header[8:12] == b'WEBP':
        return "webp"
    if header[:4] == b'\x1aE\xdf\xa3':
        return "mkv"
    if header[:4] == b'OggS':
        return "ogg"
    if header[:4] == b'fLaC':
        return "flac"
    if header[:3] == b'ID3':
        return "mp3"
    if header[:2] == b'\xff\xfb' or header[:2] == b'\xff\xf3' or header[:2] == b'\xff\xf2':
        return "mp3"
    # ISO Base Media / MP4: bytes 4-8 = "ftyp"
    if len(header) >= 8 and header[4:8] == b'ftyp':
        return "mp4"

    return "bin"


async def probe_extension(buf: io.BytesIO) -> str:
    """
    Detect the file format of *buf*, trying magic bytes first, then ffprobe.

    The buffer position is restored to 0 after probing.
    Returns a bare extension string without a dot, e.g. ``"jpg"``, ``"mp4"``.
    Falls back to ``"bin"`` if both methods fail.
    """
    ext = guess_extension_from_bytes(buf)
    if ext != "bin":
        return ext

    loop = asyncio.get_event_loop()
    ext = await loop.run_in_executor(None, _probe_extension_sync, buf.read())
    buf.seek(0)
    return ext


def _probe_extension_sync(data: bytes) -> str:
    _FORMAT_TO_EXT = {
        "mjpeg": "jpg",
        "jpeg_pipe": "jpg",
        "png_pipe": "png",
        "gif": "gif",
        "webp_pipe": "webp",
        "mp4": "mp4",
        "mov,mp4,m4a,3gp,3g2,mj2": "mp4",
        "matroska,webm": "mkv",
        "ogg": "ogg",
        "mp3": "mp3",
        "wav": "wav",
        "flac": "flac",
        "aac": "aac",
    }
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-",
            ],
            input=data,
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0:
            info = json.loads(result.stdout)
            fmt_name = info.get("format", {}).get("format_name", "")
            for key, ext in _FORMAT_TO_EXT.items():
                if key == fmt_name or key in fmt_name or fmt_name in key:
                    return ext
    except Exception as e:
        logger.warning(f"ffprobe format detection failed: {e}")
    return "bin"


def needs_video_conversion(message) -> bool:
    """
    Return True if the video message requires ffmpeg conversion to H.264/AAC/MP4.

    Checks MIME type; if it's not video/mp4 we assume conversion is needed.
    For MP4 files we optimistically assume H.264 (most Telegram-uploaded videos are).
    """
    media = message.media
    if not isinstance(media, MessageMediaDocument):
        return False
    doc = media.document
    if doc is None:
        return False

    mime = getattr(doc, "mime_type", "") or ""
    if mime == "video/mp4":
        return False  # assume H.264; ensure_faststart handles moov placement

    # Any other video MIME type needs conversion
    return mime.startswith("video/")


async def convert_video(input_buf: io.BytesIO) -> io.BytesIO:
    """
    Convert video to Telegram-compatible H.264/AAC MP4 via ffmpeg.

    Uses a temp file for output because -movflags +faststart requires seekable output.
    Runs in a thread executor to avoid blocking the event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _convert_video_sync, input_buf)


def _convert_video_sync(input_buf: io.BytesIO) -> io.BytesIO:
    input_data = input_buf.read()

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_out:
        tmp_out_path = tmp_out.name

    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", "pipe:0",
            "-c:v", "libx264",
            "-profile:v", "high",
            "-level", "4.0",
            "-pix_fmt", "yuv420p",
            "-crf", "23",
            "-preset", "fast",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ac", "2",
            "-f", "mp4",
            tmp_out_path,
        ]
        result = subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            timeout=600,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"ffmpeg conversion failed (exit {result.returncode}): "
                f"{result.stderr.decode(errors='replace')[-500:]}"
            )

        with open(tmp_out_path, "rb") as f:
            out_buf = io.BytesIO(f.read())
        out_buf.name = "video.mp4"
        out_buf.seek(0)
        return out_buf
    finally:
        try:
            os.unlink(tmp_out_path)
        except OSError:
            pass


async def ensure_faststart(input_buf: io.BytesIO) -> io.BytesIO:
    """
    Re-mux an MP4 to move the moov atom to the start (faststart) without re-encoding.

    Runs in a thread executor to avoid blocking the event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ensure_faststart_sync, input_buf)


def _ensure_faststart_sync(input_buf: io.BytesIO) -> io.BytesIO:
    input_data = input_buf.read()

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_out:
        tmp_out_path = tmp_out.name

    try:
        cmd = [
            "ffmpeg", "-y",
            "-i", "pipe:0",
            "-c", "copy",
            "-movflags", "+faststart",
            "-f", "mp4",
            tmp_out_path,
        ]
        result = subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            timeout=300,
        )
        if result.returncode != 0:
            # Non-fatal: return original buffer
            logger.warning(
                f"ensure_faststart failed (exit {result.returncode}), using original: "
                f"{result.stderr.decode(errors='replace')[-200:]}"
            )
            input_buf.seek(0)
            return input_buf

        with open(tmp_out_path, "rb") as f:
            out_buf = io.BytesIO(f.read())
        out_buf.name = "video.mp4"
        out_buf.seek(0)
        return out_buf
    finally:
        try:
            os.unlink(tmp_out_path)
        except OSError:
            pass
