import asyncio
import io
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional, Set, Tuple

from telethon import TelegramClient, utils as tl_utils
from telethon.tl import functions as tl_functions
from telethon.tl.types import (
    Message,
    MessageMediaPoll,
    DocumentAttributeVideo,
    DocumentAttributeFilename,
    InputDocumentFileLocation,
    InputFileBig,
    PeerChannel,
    PeerChat,
)

import database as db_module
import config
from sender_pool import ByteBudget, SenderPool
from media import (
    get_media_type, needs_video_conversion, convert_video, ensure_faststart,
    probe_extension, guess_extension_from_file, probe_extension_file,
)
from telegraph import find_telegraph_urls, fetch_telegraph_page, download_telegraph_images
from utils import build_message_url, retry_on_flood, rate_limit_sleep, make_upload_progress_cb

logger = logging.getLogger(__name__)

# File size limit: 2 GB non-premium, 4 GB premium
_MAX_FILE_SIZE = 4 * 1024 ** 3 if config.IS_PREMIUM else 2 * 1024 ** 3


def _sender_pool_capacity(sender_pool) -> Optional[int]:
    if sender_pool is None:
        return None

    senders = getattr(sender_pool, "_senders", None)
    if isinstance(senders, list) and senders:
        return len(senders)

    for attr in ("capacity", "_pool_size", "pool_size"):
        value = getattr(sender_pool, attr, None)
        if isinstance(value, int) and value > 0:
            return value

    return None


def _effective_upload_workers(requested_workers: int, sender_pool=None) -> int:
    workers = max(1, int(requested_workers or 1))
    capacity = _sender_pool_capacity(sender_pool)
    if capacity is not None:
        workers = min(workers, capacity)
    return max(1, workers)


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


def _replace_media_path(mf: _MediaFile, new_path: Path) -> None:
    old_path = mf.path
    mf.path = new_path
    if mf.owned and new_path != old_path:
        old_path.unlink(missing_ok=True)


def wrap_local_media_files(paths: List[Path], *, owned: bool = False) -> List[_MediaFile]:
    return [_MediaFile(Path(path), owned=owned) for path in paths]


def build_caption_messages(captions: List[str]):
    return [
        SimpleNamespace(id=index, message=caption, entities=[], media=object(), action=None)
        for index, caption in enumerate(captions)
    ]


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

    if not message.media:
        caption = _build_caption(message.message or "", source_url)
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
        sent = await upload_downloaded_message(
            userbot,
            message,
            mf,
            target_chat,
            source_url=source_url,
            media_type=media_type,
        )
    finally:
        mf.cleanup()

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

    first_id = messages[0].id
    try:
        sent = await upload_downloaded_album(
            userbot,
            valid_messages,
            mfs,
            target_chat,
            source_url=source_url,
            progress_cb=make_upload_progress_cb(f"album msg {first_id}", logger),
        )
    finally:
        for mf in mfs:
            mf.cleanup()
    return sent if isinstance(sent, list) else [sent]


async def upload_downloaded_message(
    userbot: TelegramClient,
    message: Message,
    mf: _MediaFile,
    target_chat,
    source_url: Optional[str] = None,
    media_type: Optional[str] = None,
    progress_cb=None,
    sender_pool=None,
) -> Optional[Message]:
    ul_size = mf.path.stat().st_size
    ul_timeout = max(config.OPERATION_TIMEOUT, ul_size // 524_288 + 120)
    if progress_cb is None:
        progress_cb = make_upload_progress_cb(f"msg {message.id}", logger)
    return await asyncio.wait_for(
        _upload_prepared(
            userbot,
            message,
            mf,
            target_chat,
            source_url=source_url,
            media_type=media_type,
            progress_cb=progress_cb,
            sender_pool=sender_pool,
        ),
        timeout=ul_timeout,
    )


async def upload_downloaded_album(
    userbot: TelegramClient,
    messages: List[Message],
    media_files: List[_MediaFile],
    target_chat,
    source_url: Optional[str] = None,
    progress_cb=None,
    force_document: Optional[bool] = None,
) -> Optional[List[Message]]:
    if not messages or not media_files:
        return None

    captions = []
    for i, msg in enumerate(messages):
        if source_url and i == len(messages) - 1:
            captions.append(_build_caption(msg.message or "", source_url))
        else:
            captions.append(msg.message or "")

    send_kwargs = {
        "file": [mf.path for mf in media_files],
        "caption": captions,
        "progress_callback": progress_cb,
    }
    if force_document is not None:
        send_kwargs["force_document"] = force_document

    sent = await userbot.send_file(target_chat, **send_kwargs)
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

    byte_budget = ByteBudget(cfg.MAX_INFLIGHT_BYTES)
    upload_queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.CONCURRENT_TRANSFERS)

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

    async def _cleanup_queued_items():
        while True:
            try:
                item = upload_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                if item is None:
                    continue

                kind = item[0]
                if kind == "album":
                    _, _album_messages, _valid_msgs, mfs, _source_url, bytes_held = item
                    for mf in (mfs or []):
                        mf.cleanup()
                    await byte_budget.release(bytes_held)
                elif kind == "single":
                    _, _message, mf, _source_url, bytes_held = item
                    if mf:
                        mf.cleanup()
                    await byte_budget.release(bytes_held)
            finally:
                upload_queue.task_done()

    async def _producer():
        """Iterate messages, skip already-done ones, download media, enqueue individually."""
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

                    try:
                        album_messages = await resolve_message(userbot, source_chat, message.id)
                    except Exception as e:
                        logger.error(f"Failed to resolve album at msg {message.id}: {e}")
                        await upload_queue.put(("error", message.id, None, None))
                        continue

                    media_msgs = [m for m in album_messages if m.media is not None]
                    estimated_total = sum(_get_file_size(m) or 0 for m in media_msgs)
                    await byte_budget.acquire(estimated_total)

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

                    actual_bytes = sum(mf.path.stat().st_size for mf in mfs)
                    delta = actual_bytes - estimated_total
                    if delta > 0:
                        await byte_budget.acquire(delta)
                    elif delta < 0:
                        await byte_budget.release(-delta)

                    await upload_queue.put(("album", album_messages, valid_msgs, mfs, source_url, actual_bytes))

                else:
                    # Size guard
                    if message.media and not isinstance(message.media, MessageMediaPoll):
                        file_size = _get_file_size(message)
                        if file_size and file_size > _MAX_FILE_SIZE:
                            logger.warning(
                                f"Skipping msg {message.id}: {file_size / 1024**2:.1f} MB > limit"
                            )
                            await upload_queue.put(("skip", message.id))
                            continue
                        estimated_size = file_size or 0
                    else:
                        estimated_size = 0

                    await byte_budget.acquire(estimated_size)

                    if message.media and not isinstance(message.media, MessageMediaPoll):
                        mf = await _download_to_file(userbot, message, on_progress=_touch)
                    else:
                        mf = None

                    if mf is not None:
                        actual_bytes = mf.path.stat().st_size
                        delta = actual_bytes - estimated_size
                        if delta > 0:
                            await byte_budget.acquire(delta)
                        elif delta < 0:
                            await byte_budget.release(-delta)
                        bytes_held = actual_bytes
                    else:
                        # Download failed or text-only — release reserved budget
                        if estimated_size > 0:
                            await byte_budget.release(estimated_size)
                        bytes_held = 0

                    await upload_queue.put(("single", message, mf, source_url, bytes_held))
        finally:
            await upload_queue.put(None)  # sentinel

    async def _consumer():
        nonlocal transferred, pending_records
        while True:
            item = await upload_queue.get()
            if item is None:
                break

            kind = item[0]

            if kind == "skip" or kind == "error":
                if kind == "error":
                    failed_ids.append(item[1])
                upload_queue.task_done()
                continue

            if kind == "album":
                _, album_messages, valid_msgs, mfs, source_url, bytes_held = item
                try:
                    if mfs:
                        lane_ceiling = _effective_upload_workers(
                            config.UPLOAD_WORKERS, sender_pool=_sender_pool
                        )
                        parallel_files = min(len(mfs), lane_ceiling)
                        workers_per_file = max(1, lane_ceiling // parallel_files)
                        fanout_sem = asyncio.Semaphore(parallel_files)

                        async def _preupload_album_member(i: int, mf: _MediaFile):
                            async with fanout_sem:
                                return await _upload_file_parallel(
                                    userbot,
                                    mf.path,
                                    workers_per_file,
                                    sender_pool=_sender_pool,
                                    on_progress=_make_progress_cb(
                                        f"album-pre {valid_msgs[i].id}"
                                    ),
                                )

                        pre_handles = await asyncio.gather(
                            *[
                                _preupload_album_member(i, mf)
                                for i, mf in enumerate(mfs)
                            ],
                            return_exceptions=True,
                        )

                        final_handles = []
                        final_msgs = []
                        for handle, msg in zip(pre_handles, valid_msgs):
                            if isinstance(handle, Exception):
                                logger.error(
                                    f"Pre-upload failed for album member {msg.id}: {handle}"
                                )
                            else:
                                final_handles.append(handle)
                                final_msgs.append(msg)

                        if final_handles:
                            captions = []
                            for i, msg in enumerate(final_msgs):
                                if i == len(final_msgs) - 1:
                                    captions.append(_build_caption(msg.message or "", source_url))
                                else:
                                    captions.append(msg.message or "")
                            total_bytes = sum(mf.path.stat().st_size for mf in mfs)
                            ul_timeout = max(config.OPERATION_TIMEOUT, total_bytes // 524_288 + 120)
                            sent = await asyncio.wait_for(
                                userbot.send_file(
                                    target_chat, file=final_handles, caption=captions,
                                    progress_callback=_make_progress_cb(
                                        f"album msg {album_messages[0].id}"
                                    ),
                                ),
                                timeout=ul_timeout,
                            )
                            if sent:
                                final_msg_ids = {m.id for m in final_msgs}
                                for orig_msg in final_msgs:
                                    pending_records.append((job_id, orig_msg.id, None))
                                    transferred_ids.add(orig_msg.id)
                                transferred += len(final_msgs)
                                for msg in album_messages:
                                    if msg.id not in final_msg_ids:
                                        failed_ids.append(msg.id)
                        else:
                            failed_ids.append(album_messages[0].id)
                    else:
                        failed_ids.append(album_messages[0].id)
                except Exception as e:
                    logger.error(f"Failed to upload album starting at msg {album_messages[0].id}: {e}")
                    failed_ids.append(album_messages[0].id)
                finally:
                    for mf in (mfs or []):
                        mf.cleanup()
                    await byte_budget.release(bytes_held)

            elif kind == "single":
                _, message, mf, source_url, bytes_held = item
                try:
                    ul_size = mf.path.stat().st_size if mf else 0
                    ul_timeout = max(config.OPERATION_TIMEOUT, ul_size // 524_288 + 120)
                    sent = await asyncio.wait_for(
                        _upload_prepared(
                            userbot, message, mf, target_chat, source_url,
                            progress_cb=_make_progress_cb(f"msg {message.id}"),
                            sender_pool=_sender_pool,
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
                    await byte_budget.release(bytes_held)

            upload_queue.task_done()

            # Flush batch records every 20 messages
            if len(pending_records) >= 20:
                await _flush_records()
                await db_module.update_job(
                    db, job_id, transferred=transferred, failed_ids=failed_ids
                )

            if cancel_event.is_set():
                break

            await rate_limit_sleep(cfg.TRANSFER_DELAY)

    stuck_timeout = int(getattr(cfg, "STUCK_TIMEOUT", 600))  # seconds, default 10 min

    _sender_pool = None  # assigned inside async with; captured by _consumer closure

    try:
        async with SenderPool(userbot, cfg.SENDER_POOL_SIZE) as _sender_pool:
            producer_task = asyncio.create_task(_producer())
            consumer_task = asyncio.create_task(_consumer())

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
    except Exception as exc:
        logger.error(f"Job {job_id} SenderPool error: {exc}; continuing without pool")
        # Fall back: run without a pool (SENDER_POOL_SIZE=1 emulation via main sender)
        _sender_pool = None
        producer_task = asyncio.create_task(_producer())
        consumer_task = asyncio.create_task(_consumer())

        async def _watchdog_fallback():
            while True:
                await asyncio.sleep(15)
                if producer_task.done() and consumer_task.done():
                    return
                if cancel_event.is_set():
                    await asyncio.sleep(30)
                    if not producer_task.done():
                        producer_task.cancel()
                    if not consumer_task.done():
                        consumer_task.cancel()
                    return
                idle = time.monotonic() - last_activity[0]
                if idle >= stuck_timeout:
                    logger.error(
                        f"Job {job_id} stuck for {idle:.0f}s — force-cancelling tasks"
                    )
                    producer_task.cancel()
                    consumer_task.cancel()
                    return

        watchdog_task = asyncio.create_task(_watchdog_fallback())
        try:
            try:
                await asyncio.gather(producer_task, consumer_task)
            except asyncio.CancelledError:
                pass
        finally:
            watchdog_task.cancel()
    finally:
        await _cleanup_queued_items()
        await _flush_records()
        final_status = "cancelled" if cancel_event.is_set() else "done"
        await db_module.update_job(
            db, job_id, status=final_status, transferred=transferred, failed_ids=failed_ids
        )
        logger.info(f"Job {job_id} {final_status}: {transferred}/{total} transferred, {len(failed_ids)} failed")


async def _upload_file_parallel(
    userbot: TelegramClient,
    file_path: Path,
    n_workers: int,
    sender_pool=None,
    on_progress=None,
):
    """Upload a file using N concurrent SaveBigFilePartRequest workers.

    Telethon queues all async calls through its MTProto sender, so N
    workers keep N chunk-upload requests in-flight simultaneously instead
    of waiting for each ACK before sending the next one.

    Returns an InputFileBig handle that can be passed directly to send_file().
    Falls back to the standard sequential upload_file() for small files (<10 MB).
    """
    file_size = file_path.stat().st_size

    if file_size <= 10 * 1024 * 1024:
        # Small files don't benefit enough from parallelism
        return await userbot.upload_file(str(file_path), progress_callback=on_progress)

    PART_SIZE = 512 * 1024  # 512 KB — maximum allowed by Telegram
    part_count = (file_size + PART_SIZE - 1) // PART_SIZE
    file_id = int.from_bytes(os.urandom(8), "little", signed=True)
    workers = min(
        _effective_upload_workers(n_workers, sender_pool=sender_pool),
        part_count,
    )
    uploaded = [0]

    async def _upload_worker(worker_id: int) -> None:
        # Interleaved: worker i handles parts i, i+workers, i+2*workers, ...
        # so all workers stay busy until the very last chunk.
        with open(file_path, "rb") as f:
            for part_index in range(worker_id, part_count, workers):
                f.seek(part_index * PART_SIZE)
                part = f.read(PART_SIZE)
                req = tl_functions.upload.SaveBigFilePartRequest(
                    file_id, part_index, part_count, part
                )
                if sender_pool is not None:
                    result = await sender_pool.send(req)
                else:
                    result = await userbot(req)
                if not result:
                    raise RuntimeError(
                        f"Failed to upload part {part_index} of {file_path.name}"
                    )
                uploaded[0] += len(part)
                if on_progress:
                    on_progress(uploaded[0], file_size)

    await asyncio.gather(*[_upload_worker(i) for i in range(workers)])
    return InputFileBig(file_id, part_count, file_path.name)


async def _upload_prepared(
    userbot: TelegramClient,
    message: Message,
    mf: Optional[_MediaFile],
    target_chat,
    source_url: str,
    media_type: Optional[str] = None,
    progress_cb=None,
    sender_pool=None,
) -> Optional[Message]:
    """Upload a single message given a pre-downloaded _MediaFile (or None for text-only).

    Does NOT call mf.cleanup() — the caller (consumer) is responsible for that.
    """
    if message.action:
        return None
    if isinstance(message.media, MessageMediaPoll):
        return None

    caption = _build_caption(message.message or "", source_url) if source_url else (message.message or "")

    if mf is None:
        sent = await userbot.send_message(
            target_chat,
            message=caption,
            formatting_entities=message.entities,
        )
        await _expand_telegraph(userbot, message.message, target_chat)
        return sent

    if media_type is None:
        media_type = get_media_type(message)
    if progress_cb is None:
        progress_cb = make_upload_progress_cb(f"msg {message.id}", logger)

    # For non-photo media, pre-upload the file with parallel workers so that
    # N chunk-upload requests are in-flight concurrently instead of one at a time.
    # Photos are skipped: they go through a different resize/send path.
    upload_workers = _effective_upload_workers(
        config.UPLOAD_WORKERS, sender_pool=sender_pool
    )
    if media_type not in ("photo", "video") and upload_workers > 1:
        upload_handle = await _upload_file_parallel(
            userbot, mf.path, upload_workers,
            sender_pool=sender_pool, on_progress=progress_cb,
        )
        preuploaded = True
    else:
        upload_handle = mf.path  # let send_file upload sequentially
        preuploaded = False

    if media_type == "photo":
        sent = await userbot.send_file(
            target_chat, file=upload_handle, caption=caption,
            formatting_entities=message.entities,
            force_document=False, progress_callback=progress_cb,
        )

    elif media_type == "video":
        if needs_video_conversion(message):
            logger.info(f"Converting non-H.264 video for message {message.id}")
            _replace_media_path(mf, await convert_video(mf.path))
            # Re-upload after conversion (path changed, handle is now stale)
            upload_handle = await _upload_file_parallel(
                userbot, mf.path, upload_workers,
                sender_pool=sender_pool, on_progress=progress_cb,
            )
            preuploaded = True
        else:
            _replace_media_path(mf, await ensure_faststart(mf.path))
            if upload_workers > 1:
                upload_handle = await _upload_file_parallel(
                    userbot, mf.path, upload_workers,
                    sender_pool=sender_pool, on_progress=progress_cb,
                )
                preuploaded = True
            else:
                upload_handle = mf.path
                preuploaded = False
        video_attr = _get_video_attr(message)
        duration = getattr(video_attr, "duration", 0) or 0
        w = getattr(video_attr, "w", 0) or 0
        h = getattr(video_attr, "h", 0) or 0
        sent = await userbot.send_file(
            target_chat, file=upload_handle, caption=caption,
            formatting_entities=message.entities,
            force_document=False,
            attributes=[DocumentAttributeVideo(
                duration=duration, w=w, h=h, supports_streaming=True
            )],
            progress_callback=None if preuploaded else progress_cb,
        )

    elif media_type == "voice":
        sent = await userbot.send_file(
            target_chat, file=upload_handle, caption=caption,
            formatting_entities=message.entities,
            voice_note=True,
            progress_callback=None if preuploaded else progress_cb,
        )

    elif media_type == "video_note":
        sent = await userbot.send_file(
            target_chat, file=upload_handle, caption=caption,
            video_note=True,
            progress_callback=None if preuploaded else progress_cb,
        )

    elif media_type == "animation":
        sent = await userbot.send_file(
            target_chat, file=upload_handle, caption=caption,
            formatting_entities=message.entities,
            progress_callback=None if preuploaded else progress_cb,
        )

    else:
        sent = await userbot.send_file(
            target_chat, file=upload_handle, caption=caption,
            formatting_entities=message.entities,
            force_document=True,
            progress_callback=None if preuploaded else progress_cb,
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

            image_files = await download_telegraph_images(image_urls)

            if image_files:
                try:
                    for i in range(0, len(image_files), 10):
                        batch = image_files[i : i + 10]
                        caption = content_text if i == 0 else ""
                        await userbot.send_file(
                            target_chat,
                            file=batch,
                            caption=caption,
                            progress_callback=make_upload_progress_cb(
                                f"telegraph {url[-30:]}", logger
                            ),
                        )
                finally:
                    for image_file in image_files:
                        image_file.unlink(missing_ok=True)
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


async def _parallel_download(userbot, input_location, output_path: Path,
                              file_size: int, dc_id: int,
                              n_workers: int, on_progress=None) -> None:
    """Download a document using N concurrent segment streams.

    Each worker fetches a contiguous slice of the file.  Because
    iter_download opens its own MTProto connection per generator, this
    gives N independent download streams instead of one.
    """
    CHUNK = 512 * 1024  # 512 KB per request — max allowed by Telegram
    total_chunks = (file_size + CHUNK - 1) // CHUNK
    workers = min(n_workers, total_chunks)
    chunks_per_worker = (total_chunks + workers - 1) // workers

    # Pre-allocate file so random-offset writes work
    with open(output_path, "wb") as f:
        f.seek(file_size - 1)
        f.write(b"\x00")

    downloaded = [0]

    async def fetch_segment(worker_id: int) -> None:
        start_chunk = worker_id * chunks_per_worker
        count = min(chunks_per_worker, total_chunks - start_chunk)
        if count <= 0:
            return
        offset = start_chunk * CHUNK
        with open(output_path, "r+b") as f:
            pos = offset
            async for chunk in userbot.iter_download(
                input_location,
                offset=offset,
                limit=count,
                request_size=CHUNK,
                dc_id=dc_id,
            ):
                f.seek(pos)
                f.write(chunk)
                pos += len(chunk)
                downloaded[0] += len(chunk)
                if on_progress:
                    on_progress(downloaded[0], file_size)

    await asyncio.gather(*[fetch_segment(i) for i in range(workers)])


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

        media = message.media
        if hasattr(media, "document") and media.document and file_size > 0:
            # Parallel segment download for documents (videos, large files).
            # iter_download opens a separate connection per generator, so N workers
            # = N concurrent TCP streams = N× throughput over a single connection.
            doc = media.document
            loc = InputDocumentFileLocation(
                id=doc.id,
                access_hash=doc.access_hash,
                file_reference=doc.file_reference,
                thumb_size="",
            )
            await asyncio.wait_for(
                _parallel_download(userbot, loc, tmp, file_size, doc.dc_id,
                                   config.DOWNLOAD_WORKERS, on_progress),
                timeout=dl_timeout,
            )
        else:
            # Photos and other media types — fall back to download_media
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


async def download_to_file(
    client: TelegramClient, message: Message, cache=None, on_progress=None
) -> Optional[_MediaFile]:
    return await _download_to_file(client, message, cache=cache, on_progress=on_progress)
