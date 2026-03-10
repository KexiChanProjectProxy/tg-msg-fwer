import asyncio
import io
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
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
from media import (
    get_media_type, needs_video_conversion, convert_video, ensure_faststart,
    probe_extension, guess_extension_from_file, probe_extension_file,
)
from telegraph import find_telegraph_urls, fetch_telegraph_page, download_telegraph_images
from utils import build_message_url, retry_on_flood, rate_limit_sleep, make_upload_progress_cb

logger = logging.getLogger(__name__)

# File size limit: 2 GB non-premium, 4 GB premium
_MAX_FILE_SIZE = 4 * 1024 ** 3 if config.IS_PREMIUM else 2 * 1024 ** 3


async def _iter_with_timeout(async_iter, timeout):
    """Wrap an async iterator so each __anext__() call has a per-item timeout."""
    while True:
        try:
            yield await asyncio.wait_for(async_iter.__anext__(), timeout=timeout)
        except StopAsyncIteration:
            return


@dataclass
class _MediaFile:
    """Wraps an on-disk temp file.  Call cleanup() when done to delete it."""
    path: Path
    owned: bool = True   # False for cache-hit paths we must NOT delete

    def cleanup(self) -> None:
        if self.owned:
            self.path.unlink(missing_ok=True)


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
        sent = await userbot.send_message(
            target_chat,
            message=caption,
            formatting_entities=message.entities,
        )
        await _expand_telegraph(userbot, message.message, target_chat)
        return sent

    # Size guard
    file_size = _get_file_size(message)
    if file_size and file_size > _MAX_FILE_SIZE:
        logger.warning(
            f"Skipping msg {message.id}: size {file_size / 1024**2:.1f} MB exceeds limit "
            f"({'premium' if config.IS_PREMIUM else 'non-premium'})"
        )
        return None

    media_type = get_media_type(message)
    mf = await _download_to_file(userbot, message, cache)
    if mf is None:
        return None

    try:
        pcb = make_upload_progress_cb(f"msg {message.id}", logger)

        if media_type == "photo":
            sent = await userbot.send_file(
                target_chat, file=mf.path, caption=caption,
                formatting_entities=message.entities,
                force_document=False, progress_callback=pcb,
            )

        elif media_type == "video":
            if needs_video_conversion(message):
                logger.info(f"Converting non-H.264 video for message {message.id}")
                mf.path = await convert_video(mf.path)
            else:
                mf.path = await ensure_faststart(mf.path)
            video_attr = _get_video_attr(message)
            duration = getattr(video_attr, "duration", 0) or 0
            w = getattr(video_attr, "w", 0) or 0
            h = getattr(video_attr, "h", 0) or 0
            sent = await userbot.send_file(
                target_chat, file=mf.path, caption=caption,
                formatting_entities=message.entities,
                force_document=False,
                attributes=[DocumentAttributeVideo(
                    duration=duration, w=w, h=h, supports_streaming=True
                )],
                progress_callback=pcb,
            )

        elif media_type == "voice":
            sent = await userbot.send_file(
                target_chat, file=mf.path, caption=caption,
                formatting_entities=message.entities,
                voice_note=True, progress_callback=pcb,
            )

        elif media_type == "video_note":
            sent = await userbot.send_file(
                target_chat, file=mf.path, caption=caption,
                video_note=True, progress_callback=pcb,
            )

        elif media_type == "animation":
            sent = await userbot.send_file(
                target_chat, file=mf.path, caption=caption,
                formatting_entities=message.entities, progress_callback=pcb,
            )

        else:
            sent = await userbot.send_file(
                target_chat, file=mf.path, caption=caption,
                formatting_entities=message.entities,
                force_document=True, progress_callback=pcb,
            )
    finally:
        mf.cleanup()

    await _expand_telegraph(userbot, message.message, target_chat)
    return sent


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

    # Concurrent downloads to disk
    media_messages = [msg for msg in messages if msg.media is not None]
    results = await asyncio.gather(
        *[_download_to_file(userbot, msg, cache) for msg in media_messages],
        return_exceptions=True,
    )

    mfs: List[_MediaFile] = []
    valid_messages: List[Message] = []
    for msg, result in zip(media_messages, results):
        if isinstance(result, Exception):
            logger.error(f"Failed to download album member {msg.id}: {result}")
            continue
        if result is None:
            continue
        mfs.append(result)
        valid_messages.append(msg)

    if not mfs:
        return None

    captions = []
    for i, msg in enumerate(valid_messages):
        if i == len(valid_messages) - 1:
            captions.append(_build_caption(msg.message or "", source_url))
        else:
            captions.append(msg.message or "")

    first_id = messages[0].id
    try:
        sent = await userbot.send_file(
            target_chat,
            file=[mf.path for mf in mfs],
            caption=captions,
            progress_callback=make_upload_progress_cb(f"album msg {first_id}", logger),
        )
    finally:
        for mf in mfs:
            mf.cleanup()
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

    # Load all already-transferred IDs across all jobs for this source→target pair
    _job = await db_module.get_job(db, job_id)
    transferred_ids: Set[int] = await db_module.get_transferred_ids_for_chat_pair(
        db, _job.source_chat, _job.target_chat
    )

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.CONCURRENT_TRANSFERS)

    transferred = 0
    failed_ids: List[int] = []
    seen_groups: Set[int] = set()
    pending_records: List[Tuple[int, int, Optional[int]]] = []
    last_activity = [time.monotonic()]  # updated by download/upload progress; watchdog uses this

    def _touch(current=None, total=None):
        last_activity[0] = time.monotonic()

    def _make_progress_cb(label: str):
        """Upload progress callback that updates last_activity and logs every 10%."""
        base = make_upload_progress_cb(label, logger)
        def cb(current, total):
            _touch()
            base(current, total)
        return cb

    async def _flush_records():
        nonlocal pending_records
        if pending_records:
            await db_module.record_transfers_batch(db, pending_records)
            pending_records = []

    async def _producer():
        """Iterate messages, skip already-done ones, download media, enqueue."""
        pending_singles: List[Tuple[Message, str]] = []

        async def _flush_singles():
            """Download a batch of single messages concurrently, enqueue results."""
            if not pending_singles:
                return
            # Build (msg, coro_or_None) pairs
            download_tasks = []
            for msg, _ in pending_singles:
                if msg.media and not isinstance(msg.media, MessageMediaPoll):
                    download_tasks.append((msg, _download_to_file(userbot, msg, on_progress=_touch)))
                else:
                    download_tasks.append((msg, None))

            coros = [coro for _, coro in download_tasks if coro is not None]
            results = await asyncio.gather(*coros, return_exceptions=True) if coros else []

            result_iter = iter(results)
            for (msg, coro), (_, src_url) in zip(download_tasks, pending_singles):
                if coro is None:
                    await queue.put(("single", msg, None, src_url))
                else:
                    res = next(result_iter)
                    if isinstance(res, Exception):
                        logger.error(f"Download failed for msg {msg.id}: {res}")
                        mf = None
                    else:
                        mf = res
                    await queue.put(("single", msg, mf, src_url))
            pending_singles.clear()

        try:
            async for message in _iter_with_timeout(
                userbot.iter_messages(source_chat, reverse=True), config.ITER_TIMEOUT
            ):
                _touch()
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

                    # Flush any pending singles before the album to preserve order
                    await _flush_singles()

                    # Resolve album members (may include already-transferred ones)
                    try:
                        album_messages = await resolve_message(userbot, source_chat, message.id)
                    except Exception as e:
                        logger.error(f"Failed to resolve album at msg {message.id}: {e}")
                        await queue.put(("error", message.id, None, None))
                        continue

                    # Download concurrently to disk
                    media_msgs = [m for m in album_messages if m.media is not None]
                    if media_msgs:
                        results = await asyncio.gather(
                            *[_download_to_file(userbot, m, on_progress=_touch) for m in media_msgs],
                            return_exceptions=True,
                        )
                    else:
                        results = []
                    mfs = []
                    valid_msgs = []
                    for m, res in zip(media_msgs, results):
                        if isinstance(res, Exception):
                            logger.error(f"Album download failed for msg {m.id}: {res}")
                        elif res is not None:
                            mfs.append(res)
                            valid_msgs.append(m)

                    await queue.put(("album", album_messages, valid_msgs, mfs, source_url))
                else:
                    # Size guard before batching
                    if message.media and not isinstance(message.media, MessageMediaPoll):
                        file_size = _get_file_size(message)
                        if file_size and file_size > _MAX_FILE_SIZE:
                            logger.warning(
                                f"Skipping msg {message.id}: {file_size / 1024**2:.1f} MB > limit"
                            )
                            await _flush_singles()
                            await queue.put(("skip", message.id))
                            continue

                    # Accumulate into batch; flush when batch is full
                    pending_singles.append((message, source_url))
                    if len(pending_singles) >= cfg.CONCURRENT_TRANSFERS:
                        await _flush_singles()
        finally:
            await _flush_singles()
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
                _, album_messages, valid_msgs, mfs, source_url = item
                try:
                    if mfs:
                        captions = []
                        for i, msg in enumerate(valid_msgs):
                            if i == len(valid_msgs) - 1:
                                captions.append(_build_caption(msg.message or "", source_url))
                            else:
                                captions.append(msg.message or "")
                        total_bytes = sum(mf.path.stat().st_size for mf in mfs)
                        ul_timeout = max(config.OPERATION_TIMEOUT, total_bytes // 524_288 + 120)
                        sent = await asyncio.wait_for(
                            userbot.send_file(
                                target_chat, file=[mf.path for mf in mfs], caption=captions,
                                progress_callback=_make_progress_cb(
                                    f"album msg {album_messages[0].id}"
                                ),
                            ),
                            timeout=ul_timeout,
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
                finally:
                    for mf in (mfs or []):
                        mf.cleanup()

            elif kind == "single":
                _, message, mf, source_url = item
                try:
                    ul_size = mf.path.stat().st_size if mf else 0
                    ul_timeout = max(config.OPERATION_TIMEOUT, ul_size // 524_288 + 120)
                    sent = await asyncio.wait_for(
                        _upload_prepared(
                            userbot, message, mf, target_chat, source_url,
                            progress_cb=_make_progress_cb(f"msg {message.id}"),
                        ),
                        timeout=ul_timeout,
                    )
                    if sent:
                        target_id = sent.id if not isinstance(sent, list) else None
                        pending_records.append((job_id, message.id, target_id))
                        transferred_ids.add(message.id)
                        transferred += 1
                except Exception as e:
                    logger.error(f"Failed to upload msg {message.id}: {e}")
                    failed_ids.append(message.id)
                finally:
                    if mf:
                        mf.cleanup()

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

    stuck_timeout = int(getattr(cfg, "STUCK_TIMEOUT", 600))  # seconds, default 10 min

    async def _watchdog():
        while True:
            await asyncio.sleep(15)
            if producer_task.done() and consumer_task.done():
                return
            if cancel_event.is_set():
                # Give tasks a 30s grace period to finish cleanly, then force-cancel
                await asyncio.sleep(30)
                if not producer_task.done():
                    producer_task.cancel()
                if not consumer_task.done():
                    consumer_task.cancel()
                return
            idle = time.monotonic() - last_activity[0]
            if idle >= stuck_timeout:
                logger.error(
                    f"Job {job_id} stuck for {idle:.0f}s with no I/O activity — force-cancelling tasks"
                )
                producer_task.cancel()
                consumer_task.cancel()
                return

    watchdog_task = asyncio.create_task(_watchdog())
    try:
        try:
            await asyncio.gather(producer_task, consumer_task)
        except asyncio.CancelledError:
            pass
    finally:
        watchdog_task.cancel()
        await _flush_records()
        final_status = "cancelled" if cancel_event.is_set() else "done"
        await db_module.update_job(
            db, job_id, status=final_status, transferred=transferred, failed_ids=failed_ids
        )
        logger.info(f"Job {job_id} {final_status}: {transferred}/{total} transferred, {len(failed_ids)} failed")


async def _upload_prepared(
    userbot: TelegramClient,
    message: Message,
    mf: Optional[_MediaFile],
    target_chat,
    source_url: str,
    progress_cb=None,
) -> Optional[Message]:
    """Upload a single message given a pre-downloaded _MediaFile (or None for text-only).

    Does NOT call mf.cleanup() — the caller (consumer) is responsible for that.
    """
    if message.action:
        return None
    if isinstance(message.media, MessageMediaPoll):
        return None

    caption = _build_caption(message.message or "", source_url)

    if mf is None:
        sent = await userbot.send_message(
            target_chat,
            message=caption,
            formatting_entities=message.entities,
        )
        await _expand_telegraph(userbot, message.message, target_chat)
        return sent

    media_type = get_media_type(message)
    if progress_cb is None:
        progress_cb = make_upload_progress_cb(f"msg {message.id}", logger)

    if media_type == "photo":
        sent = await userbot.send_file(
            target_chat, file=mf.path, caption=caption,
            formatting_entities=message.entities,
            force_document=False, progress_callback=progress_cb,
        )

    elif media_type == "video":
        if needs_video_conversion(message):
            logger.info(f"Converting non-H.264 video for message {message.id}")
            mf.path = await convert_video(mf.path)
        else:
            mf.path = await ensure_faststart(mf.path)
        video_attr = _get_video_attr(message)
        duration = getattr(video_attr, "duration", 0) or 0
        w = getattr(video_attr, "w", 0) or 0
        h = getattr(video_attr, "h", 0) or 0
        sent = await userbot.send_file(
            target_chat, file=mf.path, caption=caption,
            formatting_entities=message.entities,
            force_document=False,
            attributes=[DocumentAttributeVideo(
                duration=duration, w=w, h=h, supports_streaming=True
            )],
            progress_callback=progress_cb,
        )

    elif media_type == "voice":
        sent = await userbot.send_file(
            target_chat, file=mf.path, caption=caption,
            formatting_entities=message.entities,
            voice_note=True, progress_callback=progress_cb,
        )

    elif media_type == "video_note":
        sent = await userbot.send_file(
            target_chat, file=mf.path, caption=caption,
            video_note=True, progress_callback=progress_cb,
        )

    elif media_type == "animation":
        sent = await userbot.send_file(
            target_chat, file=mf.path, caption=caption,
            formatting_entities=message.entities, progress_callback=progress_cb,
        )

    else:
        sent = await userbot.send_file(
            target_chat, file=mf.path, caption=caption,
            formatting_entities=message.entities,
            force_document=True, progress_callback=progress_cb,
        )

    await _expand_telegraph(userbot, message.message, target_chat)
    return sent


async def _expand_telegraph(userbot: TelegramClient, text: str, target_chat) -> None:
    """
    If *text* contains any telegra.ph URLs, fetch each page and send its
    content (title + body text + images) to *target_chat* as follow-up messages.

    Images are batched into albums of up to 10 (Telegram limit).
    Errors are logged as warnings and do not propagate.
    """
    urls = find_telegraph_urls(text or "")
    for url in urls:
        try:
            logger.info(f"Expanding Telegraph URL: {url}")
            title, body, image_urls = await fetch_telegraph_page(url)

            header = f"**{title}**\n\n" if title else ""
            content_text = (header + body).strip()

            images = await download_telegraph_images(image_urls)

            if images:
                # Send in batches of 10; attach text to the first batch only
                for i in range(0, len(images), 10):
                    batch = images[i : i + 10]
                    caption = content_text if i == 0 else ""
                    await userbot.send_file(
                        target_chat,
                        file=batch,
                        caption=caption,
                        progress_callback=make_upload_progress_cb(
                            f"telegraph {url[-30:]}", logger
                        ),
                    )
            elif content_text:
                await userbot.send_message(target_chat, content_text)

        except Exception as e:
            logger.warning(f"Telegraph expansion failed for {url}: {e}")


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


def _get_doc_ext(message: Message) -> str:
    """Return the file extension from DocumentAttributeFilename, or empty string."""
    if hasattr(message.media, "document"):
        for attr in getattr(message.media.document, "attributes", []):
            if isinstance(attr, DocumentAttributeFilename):
                name = attr.file_name
                if "." in name:
                    return name.rsplit(".", 1)[1].lower()
    return ""


async def _download_to_file(
    userbot: TelegramClient, message: Message, cache=None, on_progress=None
) -> Optional[_MediaFile]:
    """
    Download message media to a temp file in config.TEMP_DIR (real disk, not tmpfs).

    Returns a _MediaFile whose path has the correct extension for MIME detection.
    Caller must call .cleanup() when done to delete the temp file.
    Returns None on failure.
    """
    config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    file_id = _get_file_unique_id(message)

    # --- Cache hit: hardlink into TEMP_DIR with correct extension ---
    if cache and file_id:
        cached_path = cache.get(file_id)
        if cached_path:
            ext = _get_doc_ext(message) or guess_extension_from_file(cached_path)
            link = config.TEMP_DIR / f"dl_{uuid.uuid4().hex}.{ext}"
            try:
                os.link(cached_path, link)
            except OSError:
                shutil.copy2(cached_path, link)
            return _MediaFile(link, owned=True)

    # --- Fresh download to TEMP_DIR ---
    tmp = config.TEMP_DIR / f"dl_{message.id}_{uuid.uuid4().hex}.bin"
    try:
        file_size = _get_file_size(message) or 0
        # Floor at OPERATION_TIMEOUT; add extra time assuming ≥512 KB/s minimum throughput
        dl_timeout = max(config.OPERATION_TIMEOUT, file_size // 524_288 + 120)
        await asyncio.wait_for(
            userbot.download_media(message, file=str(tmp), progress_callback=on_progress),
            timeout=dl_timeout,
        )

        ext = _get_doc_ext(message)
        if not ext:
            ext = await probe_extension_file(tmp)

        final = tmp.with_name(f"{tmp.stem}.{ext}")
        tmp.rename(final)

        if cache and file_id:
            await cache.put_file(file_id, final)

        return _MediaFile(final, owned=True)

    except Exception as e:
        logger.error(f"Failed to download media for message {message.id}: {e}")
        tmp.unlink(missing_ok=True)
        return None
