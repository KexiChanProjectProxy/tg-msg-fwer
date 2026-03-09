import asyncio
import io
import logging
from typing import List, Optional, Set, Tuple

from telethon import TelegramClient, utils as tl_utils
from telethon.tl.types import (
    Message,
    MessageMediaPoll,
    DocumentAttributeVideo,
    DocumentAttributeFilename,
    PeerChannel,
    PeerChat,
)

import database as db_module
import config
from media import get_media_type, needs_video_conversion, convert_video, ensure_faststart
from utils import build_message_url, retry_on_flood, rate_limit_sleep

logger = logging.getLogger(__name__)

# File size limit: 2 GB non-premium, 4 GB premium
_MAX_FILE_SIZE = 4 * 1024 ** 3 if config.IS_PREMIUM else 2 * 1024 ** 3


async def resolve_chat(userbot: TelegramClient, chat_ref: str):
    """Resolve a chat reference (username, URL, or numeric ID string) to an entity.

    Accepted formats
    ----------------
    - @username or plain username
    - https://t.me/username  or  t.me/c/<channel_id>/<msg_id>
    - Bot API channel ID  : -1001234567890  (strips -100, wraps in PeerChannel)
    - Raw positive ID     : 1234567890      (tried as PeerChannel first)
    - Legacy group chat ID: -456            (tried as PeerChat)
    """
    chat_ref = chat_ref.strip()

    # ── URL handling ────────────────────────────────────────────────────────
    if "t.me/" in chat_ref:
        parts = chat_ref.split("t.me/")[-1].split("/")
        if parts[0] == "c" and len(parts) > 1:
            # t.me/c/<channel_id>[/<msg_id>] — already a raw channel int
            return await userbot.get_entity(PeerChannel(int(parts[1])))
        else:
            # t.me/username[/...]
            return await userbot.get_entity(parts[0])

    # ── Numeric ID handling ─────────────────────────────────────────────────
    try:
        numeric = int(chat_ref)
    except (ValueError, TypeError):
        # Not a number — treat as username/invite link as-is
        return await userbot.get_entity(chat_ref)

    if numeric < 0:
        s = str(numeric)
        if s.startswith("-100"):
            # Bot API channel/supergroup ID: -100<real_id>
            real_id = int(s[4:])
            return await userbot.get_entity(PeerChannel(real_id))
        else:
            # Legacy group chat ID: -<chat_id>
            real_id = abs(numeric)
            return await userbot.get_entity(PeerChat(real_id))
    else:
        # Positive raw ID — assume channel/supergroup
        return await userbot.get_entity(PeerChannel(numeric))


async def resolve_message(userbot: TelegramClient, chat, msg_id: int) -> List[Message]:
    """Fetch a message. If it belongs to an album, return all album members."""
    message = await userbot.get_messages(chat, ids=msg_id)
    if message is None:
        return []
    if not message.grouped_id:
        return [message]

    surrounding = await userbot.get_messages(
        chat, min_id=max(1, msg_id - 15), max_id=msg_id + 15
    )
    album = [m for m in surrounding if m and m.grouped_id == message.grouped_id]
    album.sort(key=lambda m: m.id)
    return album


@retry_on_flood(max_retries=3)
async def transfer_one_message(
    userbot: TelegramClient,
    message: Message,
    target_chat,
    source_url: str,
    cache=None,
) -> Optional[Message]:
    """Download and re-upload a single non-album message with correct media type."""
    if message.action:
        return None

    if isinstance(message.media, MessageMediaPoll):
        logger.info(f"Skipping poll message {message.id}")
        return None

    caption = _build_caption(message.message or "", source_url)

    if not message.media:
        return await userbot.send_message(
            target_chat,
            message=caption,
            formatting_entities=message.entities,
        )

    # Size guard
    file_size = _get_file_size(message)
    if file_size and file_size > _MAX_FILE_SIZE:
        logger.warning(
            f"Skipping msg {message.id}: size {file_size / 1024**2:.1f} MB exceeds limit "
            f"({'premium' if config.IS_PREMIUM else 'non-premium'})"
        )
        return None

    media_type = get_media_type(message)

    if media_type == "photo":
        buf = await _download_to_bytes(userbot, message, cache)
        if buf is None:
            return None
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            force_document=False,
        )

    elif media_type == "video":
        buf = await _download_to_bytes(userbot, message, cache)
        if buf is None:
            return None
        if needs_video_conversion(message):
            logger.info(f"Converting non-H.264 video for message {message.id}")
            buf = await convert_video(buf)
        else:
            buf = await ensure_faststart(buf)
        video_attr = _get_video_attr(message)
        duration = getattr(video_attr, "duration", 0) or 0
        w = getattr(video_attr, "w", 0) or 0
        h = getattr(video_attr, "h", 0) or 0
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            force_document=False,
            attributes=[DocumentAttributeVideo(
                duration=duration, w=w, h=h, supports_streaming=True
            )],
        )

    elif media_type == "voice":
        buf = await _download_to_bytes(userbot, message, cache)
        if buf is None:
            return None
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            voice_note=True,
        )

    elif media_type == "video_note":
        buf = await _download_to_bytes(userbot, message, cache)
        if buf is None:
            return None
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            video_note=True,
        )

    elif media_type == "animation":
        buf = await _download_to_bytes(userbot, message, cache)
        if buf is None:
            return None
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
        )

    else:
        # Generic document (stickers, files, etc.)
        buf = await _download_to_bytes(userbot, message, cache)
        if buf is None:
            return None
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            force_document=True,
        )


@retry_on_flood(max_retries=3)
async def transfer_album(
    userbot: TelegramClient,
    messages: List[Message],
    target_chat,
    source_url: str,
    cache=None,
) -> Optional[List[Message]]:
    """Download all album media concurrently and re-upload as a grouped album."""
    if not messages:
        return None

    # Concurrent downloads
    download_tasks = [
        _download_to_bytes(userbot, msg, cache)
        for msg in messages
        if msg.media is not None
    ]
    media_messages = [msg for msg in messages if msg.media is not None]

    results = await asyncio.gather(*download_tasks, return_exceptions=True)

    media_list: List[io.BytesIO] = []
    valid_messages: List[Message] = []
    for msg, result in zip(media_messages, results):
        if isinstance(result, Exception):
            logger.error(f"Failed to download album member {msg.id}: {result}")
            continue
        if result is None:
            continue
        media_list.append(result)
        valid_messages.append(msg)

    if not media_list:
        return None

    captions = []
    for i, msg in enumerate(valid_messages):
        if i == len(valid_messages) - 1:
            captions.append(_build_caption(msg.message or "", source_url))
        else:
            captions.append(msg.message or "")

    sent = await userbot.send_file(
        target_chat,
        file=media_list,
        caption=captions,
    )
    return sent if isinstance(sent, list) else [sent]


async def transfer_bulk_files(
    userbot: TelegramClient,
    source_chat,
    target_chat,
    job_id: int,
    db,
    cfg,
    cancel_event: asyncio.Event,
):
    """Transfer only media messages from source to target, oldest first.

    Wraps transfer_bulk with media_only=True.
    """
    await transfer_bulk(
        userbot, source_chat, target_chat, job_id, db, cfg, cancel_event, media_only=True
    )


async def transfer_bulk(
    userbot: TelegramClient,
    source_chat,
    target_chat,
    job_id: int,
    db,
    cfg,
    cancel_event: asyncio.Event,
    media_only: bool = False,
):
    """
    Transfer messages from source to target using a producer-consumer pipeline.

    The producer iterates messages oldest-first and downloads media, putting prepared
    items onto a bounded queue. The consumer takes from the queue, uploads, and records
    in the DB. Download of message N+1 overlaps with upload of message N for throughput.

    When media_only=True, text-only messages are skipped; all media types and albums
    are transferred in their original channel order.
    """
    total_list = await userbot.get_messages(source_chat, limit=1)
    total = total_list.total
    await db_module.update_job(db, job_id, status="running", total=total)

    # Load all already-transferred IDs upfront (one query instead of per-message checks)
    transferred_ids: Set[int] = await db_module.get_transferred_ids(db, job_id)

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.CONCURRENT_TRANSFERS)

    transferred = 0
    failed_ids: List[int] = []
    seen_groups: Set[int] = set()
    pending_records: List[Tuple[int, int, Optional[int]]] = []

    async def _flush_records():
        nonlocal pending_records
        if pending_records:
            await db_module.record_transfers_batch(db, pending_records)
            pending_records = []

    async def _producer():
        """Iterate messages, skip already-done ones, download media, enqueue."""
        try:
            async for message in userbot.iter_messages(source_chat, reverse=True):
                if cancel_event.is_set():
                    break
                if message.action:
                    continue
                if message.id in transferred_ids:
                    continue

                # In media-only mode skip text messages (albums always have media)
                if media_only and not message.media and not message.grouped_id:
                    continue

                source_url = build_message_url(source_chat, message.id)

                if message.grouped_id:
                    if message.grouped_id in seen_groups:
                        continue
                    seen_groups.add(message.grouped_id)
                    # Resolve album members (may include already-transferred ones)
                    try:
                        album_messages = await resolve_message(userbot, source_chat, message.id)
                    except Exception as e:
                        logger.error(f"Failed to resolve album at msg {message.id}: {e}")
                        await queue.put(("error", message.id, None, None))
                        continue

                    # Download concurrently
                    download_tasks = [
                        _download_to_bytes(userbot, m)
                        for m in album_messages
                        if m.media is not None
                    ]
                    media_msgs = [m for m in album_messages if m.media is not None]
                    if download_tasks:
                        results = await asyncio.gather(*download_tasks, return_exceptions=True)
                    else:
                        results = []
                    bufs = []
                    valid_msgs = []
                    for m, res in zip(media_msgs, results):
                        if isinstance(res, Exception):
                            logger.error(f"Album download failed for msg {m.id}: {res}")
                        elif res is not None:
                            bufs.append(res)
                            valid_msgs.append(m)

                    await queue.put(("album", album_messages, valid_msgs, bufs, source_url))
                else:
                    # Single message: download media if any
                    buf = None
                    if message.media and not isinstance(message.media, MessageMediaPoll):
                        file_size = _get_file_size(message)
                        if file_size and file_size > _MAX_FILE_SIZE:
                            logger.warning(
                                f"Skipping msg {message.id}: {file_size / 1024**2:.1f} MB > limit"
                            )
                            await queue.put(("skip", message.id))
                            continue
                        buf = await _download_to_bytes(userbot, message)
                    await queue.put(("single", message, buf, source_url))
        finally:
            await queue.put(None)  # sentinel

    async def _consumer():
        nonlocal transferred, pending_records
        while True:
            item = await queue.get()
            if item is None:
                break

            kind = item[0]

            if kind == "skip" or kind == "error":
                if kind == "error":
                    failed_ids.append(item[1])
                queue.task_done()
                continue

            if kind == "album":
                _, album_messages, valid_msgs, bufs, source_url = item
                try:
                    if bufs:
                        captions = []
                        for i, msg in enumerate(valid_msgs):
                            if i == len(valid_msgs) - 1:
                                captions.append(_build_caption(msg.message or "", source_url))
                            else:
                                captions.append(msg.message or "")
                        sent = await userbot.send_file(
                            target_chat, file=bufs, caption=captions
                        )
                        if sent:
                            for orig_msg in album_messages:
                                pending_records.append((job_id, orig_msg.id, None))
                                transferred_ids.add(orig_msg.id)
                            transferred += len(album_messages)
                    else:
                        failed_ids.append(album_messages[0].id)
                except Exception as e:
                    logger.error(f"Failed to upload album starting at msg {album_messages[0].id}: {e}")
                    failed_ids.append(album_messages[0].id)

            elif kind == "single":
                _, message, buf, source_url = item
                try:
                    sent = await _upload_prepared(userbot, message, buf, target_chat, source_url)
                    if sent:
                        target_id = sent.id if not isinstance(sent, list) else None
                        pending_records.append((job_id, message.id, target_id))
                        transferred_ids.add(message.id)
                        transferred += 1
                except Exception as e:
                    logger.error(f"Failed to upload msg {message.id}: {e}")
                    failed_ids.append(message.id)

            queue.task_done()

            # Flush batch records every 20 messages
            if len(pending_records) >= 20:
                await _flush_records()
                await db_module.update_job(
                    db, job_id, transferred=transferred, failed_ids=failed_ids
                )

            if cancel_event.is_set():
                break

            await rate_limit_sleep(cfg.TRANSFER_DELAY)

    producer_task = asyncio.create_task(_producer())
    consumer_task = asyncio.create_task(_consumer())

    await asyncio.gather(producer_task, consumer_task)

    await _flush_records()

    final_status = "cancelled" if cancel_event.is_set() else "done"
    await db_module.update_job(
        db, job_id, status=final_status, transferred=transferred, failed_ids=failed_ids
    )
    logger.info(f"Job {job_id} {final_status}: {transferred}/{total} transferred, {len(failed_ids)} failed")


async def _upload_prepared(
    userbot: TelegramClient,
    message: Message,
    buf: Optional[io.BytesIO],
    target_chat,
    source_url: str,
) -> Optional[Message]:
    """Upload a single message given a pre-downloaded buffer (or None for text-only)."""
    if message.action:
        return None
    if isinstance(message.media, MessageMediaPoll):
        return None

    caption = _build_caption(message.message or "", source_url)

    if buf is None:
        return await userbot.send_message(
            target_chat,
            message=caption,
            formatting_entities=message.entities,
        )

    media_type = get_media_type(message)

    if media_type == "photo":
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            force_document=False,
        )

    elif media_type == "video":
        if needs_video_conversion(message):
            logger.info(f"Converting non-H.264 video for message {message.id}")
            buf = await convert_video(buf)
        else:
            buf = await ensure_faststart(buf)
        video_attr = _get_video_attr(message)
        duration = getattr(video_attr, "duration", 0) or 0
        w = getattr(video_attr, "w", 0) or 0
        h = getattr(video_attr, "h", 0) or 0
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            force_document=False,
            attributes=[DocumentAttributeVideo(
                duration=duration, w=w, h=h, supports_streaming=True
            )],
        )

    elif media_type == "voice":
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            voice_note=True,
        )

    elif media_type == "video_note":
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            video_note=True,
        )

    elif media_type == "animation":
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
        )

    else:
        return await userbot.send_file(
            target_chat,
            file=buf,
            caption=caption,
            formatting_entities=message.entities,
            force_document=True,
        )


def _build_caption(text: str, source_url: str) -> str:
    return (text or "") + f"\n\nSource: {source_url}"


def _get_file_size(message: Message) -> Optional[int]:
    """Return the file size in bytes for a document media, or None."""
    media = message.media
    if hasattr(media, "document") and media.document:
        return getattr(media.document, "size", None)
    return None


def _get_video_attr(message: Message) -> Optional[DocumentAttributeVideo]:
    """Return the DocumentAttributeVideo from a message's document attributes, or None."""
    media = message.media
    if not hasattr(media, "document") or not media.document:
        return None
    for attr in (media.document.attributes or []):
        if isinstance(attr, DocumentAttributeVideo):
            return attr
    return None


def _get_file_unique_id(message: Message) -> Optional[str]:
    """Return a stable unique ID for the media attached to message, or None."""
    media = message.media
    if media is None:
        return None
    if hasattr(media, "photo") and media.photo:
        return f"photo_{media.photo.id}"
    if hasattr(media, "document") and media.document:
        return f"doc_{media.document.id}"
    return None


async def _download_to_bytes(userbot: TelegramClient, message: Message, cache=None) -> Optional[io.BytesIO]:
    """Download message media into a BytesIO buffer, using cache when available."""
    file_id = _get_file_unique_id(message)

    if cache and file_id:
        cached_path = cache.get(file_id)
        if cached_path:
            buf = io.BytesIO(cached_path.read_bytes())
            # Re-attach filename hint
            if hasattr(message.media, "document"):
                for attr in getattr(message.media.document, "attributes", []):
                    if isinstance(attr, DocumentAttributeFilename):
                        buf.name = attr.file_name
                        break
            return buf

    try:
        buf = io.BytesIO()
        await userbot.download_media(message, file=buf)
        buf.seek(0)
        # Attach filename hint so Telethon picks the right MIME type on re-upload
        if hasattr(message.media, "document"):
            for attr in getattr(message.media.document, "attributes", []):
                if isinstance(attr, DocumentAttributeFilename):
                    buf.name = attr.file_name
                    break

        if cache and file_id:
            await cache.put(file_id, buf.getvalue())
            buf.seek(0)

        return buf
    except Exception as e:
        logger.error(f"Failed to download media for message {message.id}: {e}")
        return None
