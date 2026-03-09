"""Media type detection and ffmpeg conversion utilities for Telegram transfers."""

import asyncio
import io
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
