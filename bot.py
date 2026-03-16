import asyncio
import io
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from telethon import TelegramClient, events
from telethon.tl.custom import Button
from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo, Message

import config
import database as db_module
from archive import is_archive, extract_archive, classify_media
from media import get_media_type, probe_extension
from models import TransferStatus
from telegraph import find_telegraph_urls, fetch_telegraph_page, download_telegraph_images
from transfer import resolve_chat, resolve_message, transfer_one_message, transfer_album, transfer_bulk, transfer_bulk_files
from utils import build_message_url

logger = logging.getLogger(__name__)

# job_id -> (asyncio.Task, asyncio.Event)
active_tasks: Dict[int, Tuple[asyncio.Task, asyncio.Event]] = {}

# user_id -> forwarded Message (or album List[Message]) awaiting a target channel
pending_forwards: Dict[int, Union[Message, List[Message]]] = {}


def is_admin(user_id: int) -> bool:
    return user_id in config.ADMIN_IDS


def register_handlers(bot: TelegramClient, userbot: TelegramClient, db, file_cache=None):

    ALBUM_COLLECT_DELAY = 0.5  # seconds to wait for all album members
    _album_buffer: Dict[int, List[Message]] = {}   # grouped_id -> messages
    _album_timers: Dict[int, asyncio.Task] = {}    # grouped_id -> debounce task

    @bot.on(events.NewMessage(pattern="/start"))
    async def cmd_start(event):
        if not is_admin(event.sender_id):
            return await event.reply("Access denied.")
        buttons = [
            [Button.inline("Transfer single message", b"help_transfer")],
            [Button.inline("Bulk channel transfer", b"help_bulk")],
            [Button.inline("Help", b"help_main")],
        ]
        await event.reply(
            "Welcome to Telegram Reload!\n\n"
            "I can help you transfer messages between channels, "
            "preserving media and appending source URLs.",
            buttons=buttons,
        )

    @bot.on(events.CallbackQuery(data=b"help_transfer"))
    async def cb_help_transfer(event):
        await event.answer()
        await event.reply(
            "**Single message transfer:**\n"
            "`/transfer <message_url> <target_chat>`\n\n"
            "Example:\n"
            "`/transfer https://t.me/somechannel/123 @targetchannel`"
        )

    @bot.on(events.CallbackQuery(data=b"help_bulk"))
    async def cb_help_bulk(event):
        await event.answer()
        await event.reply(
            "**Bulk channel transfer:**\n"
            "`/bulk <source_chat> <target_chat>`\n\n"
            "Example:\n"
            "`/bulk @sourcechannel @targetchannel`\n\n"
            "Use `/status` to check progress and `/cancel <job_id>` to stop."
        )

    @bot.on(events.CallbackQuery(data=b"help_main"))
    async def cb_help_main(event):
        await event.answer()
        await _send_help(event)

    @bot.on(events.NewMessage(pattern="/help"))
    async def cmd_help(event):
        if not is_admin(event.sender_id):
            return
        await _send_help(event)

    async def _send_help(event):
        await event.reply(
            "**Commands:**\n"
            "`/transfer <url> <target>` — Transfer a single message\n"
            "`/bulk <source> <target>` — Transfer entire channel\n"
            "`/bulkfiles <source> <target>` — Transfer media-only (oldest first)\n"
            "`/status [job_id]` — Show job progress\n"
            "`/cancel <job_id>` — Cancel a bulk job\n"
            "`/resume <job_id>` — Resume an interrupted or failed job\n"
            "`/help` — Show this message\n\n"
            "You can also send or forward any file to the bot and reply with a "
            "target channel to upload it there."
        )

    @bot.on(events.NewMessage(pattern=r"/transfer (.+)"))
    async def cmd_transfer(event):
        if not is_admin(event.sender_id):
            return await event.reply("Access denied.")

        args = event.pattern_match.group(1).split()
        if len(args) < 2:
            return await event.reply("Usage: `/transfer <message_url> <target_chat>`")

        source_url_arg, target_ref = args[0], args[1]
        status_msg = await event.reply("Resolving message...")

        try:
            chat_ref, msg_id = _parse_message_url(source_url_arg)
            if not chat_ref or not msg_id:
                return await status_msg.edit("Could not parse message URL.")

            source_chat = await resolve_chat(userbot, chat_ref)
            target_chat = await resolve_chat(userbot, target_ref)

            await status_msg.edit("Fetching message...")
            messages = await resolve_message(userbot, source_chat, msg_id)

            if not messages:
                return await status_msg.edit("Message not found.")

            url = build_message_url(source_chat, msg_id)
            await status_msg.edit("Transferring...")

            if len(messages) == 1:
                sent = await transfer_one_message(userbot, messages[0], target_chat, url)
                if sent:
                    await status_msg.edit(f"Done! Transferred message {msg_id}.")
                else:
                    await status_msg.edit("Skipped (unsupported message type).")
            else:
                sent = await transfer_album(userbot, messages, target_chat, url)
                if sent:
                    await status_msg.edit(
                        f"Done! Transferred album ({len(messages)} media files)."
                    )
                else:
                    await status_msg.edit("Failed to transfer album.")

        except Exception as e:
            logger.error(f"Transfer error: {e}", exc_info=True)
            await status_msg.edit(f"Error: {e}")

    @bot.on(events.NewMessage(pattern=r"/bulk (.+)"))
    async def cmd_bulk(event):
        if not is_admin(event.sender_id):
            return await event.reply("Access denied.")

        args = event.pattern_match.group(1).split()
        if len(args) < 2:
            return await event.reply("Usage: `/bulk <source_chat> <target_chat>`")

        source_ref, target_ref = args[0], args[1]
        status_msg = await event.reply("Starting bulk transfer...")

        try:
            source_chat = await resolve_chat(userbot, source_ref)
            target_chat = await resolve_chat(userbot, target_ref)

            job_id = await db_module.create_job(db, event.sender_id, source_ref, target_ref)
            cancel_event = asyncio.Event()

            async def run_bulk():
                try:
                    await transfer_bulk(
                        userbot, source_chat, target_chat, job_id, db, config, cancel_event
                    )
                    job = await db_module.get_job(db, job_id)
                    try:
                        await bot.send_message(
                            event.sender_id,
                            f"Bulk transfer job #{job_id} completed: "
                            f"{job.transferred}/{job.total} transferred, "
                            f"{len(job.failed_ids)} failed.",
                        )
                    except Exception:
                        pass
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Bulk job {job_id} error: {e}", exc_info=True)
                    await db_module.update_job(db, job_id, status="failed", error=str(e))
                finally:
                    active_tasks.pop(job_id, None)

            task = asyncio.create_task(run_bulk())
            active_tasks[job_id] = (task, cancel_event)

            await status_msg.edit(
                f"Bulk transfer started! Job ID: `{job_id}`\n"
                f"Source: {source_ref} → Target: {target_ref}\n"
                f"Use `/status {job_id}` to check progress."
            )

        except Exception as e:
            logger.error(f"Bulk start error: {e}", exc_info=True)
            await status_msg.edit(f"Error: {e}")

    @bot.on(events.NewMessage(pattern=r"/bulkfiles (.+)"))
    async def cmd_bulkfiles(event):
        if not is_admin(event.sender_id):
            return await event.reply("Access denied.")

        args = event.pattern_match.group(1).split()
        if len(args) < 2:
            return await event.reply("Usage: `/bulkfiles <source_chat> <target_chat>`")

        source_ref, target_ref = args[0], args[1]
        status_msg = await event.reply("Starting media-only bulk transfer...")

        try:
            source_chat = await resolve_chat(userbot, source_ref)
            target_chat = await resolve_chat(userbot, target_ref)

            job_id = await db_module.create_job(db, event.sender_id, source_ref, target_ref)
            cancel_event = asyncio.Event()

            async def run_bulkfiles():
                try:
                    await transfer_bulk_files(
                        userbot, source_chat, target_chat, job_id, db, config, cancel_event
                    )
                    job = await db_module.get_job(db, job_id)
                    try:
                        await bot.send_message(
                            event.sender_id,
                            f"Media-only bulk transfer job #{job_id} completed: "
                            f"{job.transferred} transferred, "
                            f"{len(job.failed_ids)} failed.",
                        )
                    except Exception:
                        pass
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Bulkfiles job {job_id} error: {e}", exc_info=True)
                    await db_module.update_job(db, job_id, status="failed", error=str(e))
                finally:
                    active_tasks.pop(job_id, None)

            task = asyncio.create_task(run_bulkfiles())
            active_tasks[job_id] = (task, cancel_event)

            await status_msg.edit(
                f"Media-only bulk transfer started! Job ID: `{job_id}`\n"
                f"Source: {source_ref} → Target: {target_ref}\n"
                f"Files will be transferred oldest-first.\n"
                f"Use `/status {job_id}` to check progress."
            )

        except Exception as e:
            logger.error(f"Bulkfiles start error: {e}", exc_info=True)
            await status_msg.edit(f"Error: {e}")

    @bot.on(events.NewMessage(pattern=r"/status ?(\d*)"))
    async def cmd_status(event):
        if not is_admin(event.sender_id):
            return

        job_id_str = event.pattern_match.group(1)

        if job_id_str:
            job = await db_module.get_job(db, int(job_id_str))
            if not job:
                return await event.reply(f"Job #{job_id_str} not found.")
            await event.reply(_format_job(job))
        else:
            jobs = await db_module.get_user_jobs(db, event.sender_id)
            if not jobs:
                return await event.reply("No jobs found.")
            text = "\n\n".join(_format_job(j) for j in jobs[:5])
            await event.reply(text)

    @bot.on(events.NewMessage(pattern=r"/cancel (\d+)"))
    async def cmd_cancel(event):
        if not is_admin(event.sender_id):
            return

        job_id = int(event.pattern_match.group(1))

        if job_id in active_tasks:
            _, cancel_event = active_tasks[job_id]
            cancel_event.set()
            return await event.reply(
                f"Cancellation requested for job #{job_id}. "
                "It will stop after the current transfer completes."
            )

        # Not actively running — check DB and cancel there if appropriate
        job = await db_module.get_job(db, job_id)
        if not job:
            return await event.reply(f"Job #{job_id} not found.")
        if job.status in (TransferStatus.DONE, TransferStatus.CANCELLED):
            return await event.reply(f"Job #{job_id} is already {job.status.value}.")
        await db_module.update_job(db, job_id, status="cancelled")
        await event.reply(f"Job #{job_id} marked as cancelled.")

    @bot.on(events.NewMessage(pattern=r"/resume (\d+)"))
    async def cmd_resume(event):
        if not is_admin(event.sender_id):
            return

        job_id = int(event.pattern_match.group(1))
        job = await db_module.get_job(db, job_id)

        if not job:
            return await event.reply(f"Job #{job_id} not found.")
        if job.status not in (TransferStatus.INTERRUPTED, TransferStatus.FAILED):
            return await event.reply(
                f"Job #{job_id} has status '{job.status.value}' — can only resume interrupted or failed jobs."
            )
        if job_id in active_tasks:
            return await event.reply(f"Job #{job_id} is already running.")

        status_msg = await event.reply(f"Resuming job #{job_id}...")

        try:
            source_chat = await resolve_chat(userbot, job.source_chat)
            target_chat = await resolve_chat(userbot, job.target_chat)
            cancel_event = asyncio.Event()

            async def run_resume():
                try:
                    await transfer_bulk(
                        userbot, source_chat, target_chat, job_id, db, config, cancel_event
                    )
                    job_updated = await db_module.get_job(db, job_id)
                    try:
                        await bot.send_message(
                            event.sender_id,
                            f"Bulk transfer job #{job_id} completed: "
                            f"{job_updated.transferred}/{job_updated.total} transferred, "
                            f"{len(job_updated.failed_ids)} failed.",
                        )
                    except Exception:
                        pass
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Resume job {job_id} error: {e}", exc_info=True)
                    await db_module.update_job(db, job_id, status="failed", error=str(e))
                finally:
                    active_tasks.pop(job_id, None)

            task = asyncio.create_task(run_resume())
            active_tasks[job_id] = (task, cancel_event)

            await status_msg.edit(
                f"Job #{job_id} resumed!\n"
                f"Source: {job.source_chat} → Target: {job.target_chat}\n"
                f"Use `/status {job_id}` to check progress."
            )

        except Exception as e:
            logger.error(f"Resume start error: {e}", exc_info=True)
            await status_msg.edit(f"Error: {e}")

    # ── helpers used by the forward handler ─────────────────────────────────

    async def _transfer_to_default(fwd_message, target_ref: str, status_msg) -> bool:
        """
        Transfer *fwd_message* to the channel identified by *target_ref*.

        Tries the original channel source first; falls back to direct download.
        Returns True on success.
        """
        try:
            target_chat = await resolve_chat(userbot, target_ref)
        except Exception as e:
            await status_msg.edit(f"Could not resolve default channel {target_ref!r}: {e}")
            return False

        fwd_from = fwd_message.fwd_from
        channel_id = getattr(fwd_from, "channel_id", None)
        channel_post = getattr(fwd_from, "channel_post", None)

        if channel_id and channel_post:
            await status_msg.edit("Fetching original message from source channel...")
            source_ref = f"-100{channel_id}"
            try:
                source_chat = await resolve_chat(userbot, source_ref)
                messages = await resolve_message(userbot, source_chat, channel_post)
            except Exception:
                messages = []

            if messages:
                source_url = build_message_url(source_chat, channel_post)
                await status_msg.edit("Transferring...")
                if len(messages) == 1:
                    await transfer_one_message(userbot, messages[0], target_chat, source_url, cache=file_cache)
                else:
                    await transfer_album(userbot, messages, target_chat, source_url, cache=file_cache)
                await status_msg.edit("Done!")
                return True
            # Fall through to direct download

        await status_msg.edit("Downloading media...")
        media_type = get_media_type(fwd_message)
        buf = io.BytesIO()
        await bot.download_media(fwd_message, file=buf)
        buf.seek(0)
        if hasattr(fwd_message.media, "document"):
            for attr in getattr(fwd_message.media.document, "attributes", []):
                if isinstance(attr, DocumentAttributeFilename):
                    buf.name = attr.file_name
                    break
        if not getattr(buf, "name", None):
            ext = await probe_extension(buf)
            buf.name = f"file.{ext}"

        await status_msg.edit("Uploading...")
        if media_type == "photo":
            await userbot.send_file(target_chat, file=buf, force_document=False)
        elif media_type == "video":
            video_attr = None
            if hasattr(fwd_message.media, "document"):
                for attr in getattr(fwd_message.media.document, "attributes", []):
                    if isinstance(attr, DocumentAttributeVideo):
                        video_attr = attr
                        break
            duration = getattr(video_attr, "duration", 0) or 0
            w = getattr(video_attr, "w", 0) or 0
            h = getattr(video_attr, "h", 0) or 0
            await userbot.send_file(
                target_chat, file=buf, force_document=False,
                attributes=[DocumentAttributeVideo(duration=duration, w=w, h=h, supports_streaming=True)],
            )
        elif media_type == "voice":
            await userbot.send_file(target_chat, file=buf, voice_note=True)
        elif media_type == "video_note":
            await userbot.send_file(target_chat, file=buf, video_note=True)
        else:
            await userbot.send_file(target_chat, file=buf, force_document=True)
        await status_msg.edit("Done!")
        return True

    async def _transfer_album_to_default(album_messages: List[Message], target_ref: str, status_msg) -> bool:
        """
        Transfer an album (list of grouped messages) to the channel identified by target_ref.
        Tries the original channel source first (once for the whole album); falls back to
        downloading all members from the bot and sending them as a grouped album.
        Returns True on success.
        """
        try:
            target_chat = await resolve_chat(userbot, target_ref)
        except Exception as e:
            await status_msg.edit(f"Could not resolve channel {target_ref!r}: {e}")
            return False

        first = album_messages[0]
        fwd_from = first.fwd_from
        channel_id = getattr(fwd_from, "channel_id", None)
        channel_post = getattr(fwd_from, "channel_post", None)

        if channel_id and channel_post:
            await status_msg.edit("Fetching original album from source channel...")
            source_ref = f"-100{channel_id}"
            try:
                source_chat = await resolve_chat(userbot, source_ref)
                messages = await resolve_message(userbot, source_chat, channel_post)
            except Exception:
                messages = []

            if messages:
                source_url = build_message_url(source_chat, channel_post)
                await status_msg.edit("Transferring album...")
                if len(messages) == 1:
                    await transfer_one_message(userbot, messages[0], target_chat, source_url, cache=file_cache)
                else:
                    await transfer_album(userbot, messages, target_chat, source_url, cache=file_cache)
                await status_msg.edit("Done!")
                return True
            # Fall through to direct download

        # Fallback: download all members from bot, send as grouped album
        await status_msg.edit(f"Downloading {len(album_messages)} media file(s)...")
        bufs = []
        for msg in album_messages:
            buf = io.BytesIO()
            await bot.download_media(msg, file=buf)
            buf.seek(0)
            if hasattr(msg.media, "document"):
                for attr in getattr(msg.media.document, "attributes", []):
                    if isinstance(attr, DocumentAttributeFilename):
                        buf.name = attr.file_name
                        break
            if not getattr(buf, "name", None):
                ext = await probe_extension(buf)
                buf.name = f"file.{ext}"
            bufs.append(buf)

        await status_msg.edit("Uploading album...")
        await userbot.send_file(target_chat, file=bufs, force_document=False)
        await status_msg.edit("Done!")
        return True

    async def _process_album_after_delay(grouped_id: int, sender_id: int):
        """Debounce handler: wait for all album members, then route them together."""
        await asyncio.sleep(ALBUM_COLLECT_DELAY)
        messages = _album_buffer.pop(grouped_id, [])
        _album_timers.pop(grouped_id, None)
        if not messages:
            return

        messages.sort(key=lambda m: m.id)
        first = messages[0]
        default_ch = _default_channel_for(first)

        if default_ch:
            status_msg = await bot.send_message(
                sender_id,
                f"Auto-routing album ({len(messages)} media) to default channel…"
            )
            try:
                await _transfer_album_to_default(messages, default_ch, status_msg)
            except Exception as e:
                logger.error(f"Album auto-route error: {e}", exc_info=True)
                await status_msg.edit(f"Error: {e}")
        else:
            pending_forwards[sender_id] = messages
            await bot.send_message(
                sender_id,
                f"Received album with {len(messages)} media file(s). "
                "Where should I send it? Reply with the channel username or URL."
            )

    async def _handle_telegraph_urls(urls: List[str], status_msg) -> bool:
        """Fetch Telegraph articles and upload images to DEFAULT_IMAGE_CHANNEL."""
        if not config.DEFAULT_IMAGE_CHANNEL:
            return False
        try:
            target_chat = await resolve_chat(userbot, config.DEFAULT_IMAGE_CHANNEL)
        except Exception as e:
            await status_msg.edit(f"Could not resolve DEFAULT_IMAGE_CHANNEL: {e}")
            return False

        for url in urls:
            await status_msg.edit(f"Fetching Telegraph article…")
            try:
                title, _body, image_urls = await fetch_telegraph_page(url)
            except Exception as e:
                logger.warning(f"Could not fetch Telegraph page {url}: {e}")
                continue

            if not image_urls:
                logger.info(f"No images in Telegraph article {url}")
                continue

            await status_msg.edit(f"Downloading {len(image_urls)} image(s) from {title!r}…")
            bufs = await download_telegraph_images(image_urls)
            if not bufs:
                continue

            caption = f"<b>{title}</b>\n{url}" if title else url

            # Upload in albums of up to 10
            for chunk_start in range(0, len(bufs), 10):
                chunk = bufs[chunk_start:chunk_start + 10]
                chunk_caption = caption if chunk_start == 0 else ""
                await userbot.send_file(target_chat, file=chunk, caption=chunk_caption, parse_mode="html")

        await status_msg.edit("Done!")
        return True

    async def _handle_archive(fwd_message, status_msg) -> bool:
        """Download archive, extract, route images and videos to default channels."""
        if not config.DEFAULT_IMAGE_CHANNEL and not config.DEFAULT_VIDEO_CHANNEL:
            return False

        tmp_archive = config.TEMP_DIR / f"archive_{fwd_message.id}"
        tmp_extract = config.TEMP_DIR / f"extract_{fwd_message.id}"
        try:
            config.TEMP_DIR.mkdir(parents=True, exist_ok=True)

            # Determine filename
            fname = f"archive_{fwd_message.id}"
            doc = getattr(fwd_message.media, "document", None)
            if doc:
                for attr in getattr(doc, "attributes", []):
                    if hasattr(attr, "file_name") and attr.file_name:
                        fname = attr.file_name
                        break
            tmp_archive = config.TEMP_DIR / fname

            await status_msg.edit("Downloading archive…")
            await bot.download_media(fwd_message, file=str(tmp_archive))

            await status_msg.edit("Extracting archive…")
            await extract_archive(tmp_archive, tmp_extract)

            images, videos = classify_media(tmp_extract)
            logger.info(f"Archive contains {len(images)} images, {len(videos)} videos")

            if images and config.DEFAULT_IMAGE_CHANNEL:
                img_chat = await resolve_chat(userbot, config.DEFAULT_IMAGE_CHANNEL)
                await status_msg.edit(f"Uploading {len(images)} image(s)…")
                for chunk_start in range(0, len(images), 10):
                    chunk = [str(p) for p in images[chunk_start:chunk_start + 10]]
                    await userbot.send_file(img_chat, file=chunk)

            if videos and config.DEFAULT_VIDEO_CHANNEL:
                vid_chat = await resolve_chat(userbot, config.DEFAULT_VIDEO_CHANNEL)
                await status_msg.edit(f"Uploading {len(videos)} video(s)…")
                for chunk_start in range(0, len(videos), 10):
                    chunk = [str(p) for p in videos[chunk_start:chunk_start + 10]]
                    await userbot.send_file(vid_chat, file=chunk, force_document=False)

            await status_msg.edit("Done!")
            return True

        finally:
            if tmp_archive.exists():
                tmp_archive.unlink(missing_ok=True)
            if tmp_extract.exists():
                shutil.rmtree(tmp_extract, ignore_errors=True)

    def _default_channel_for(fwd_message) -> Optional[str]:
        """Return the configured default channel for the media type, or None.

        Resolution order: type-specific channel → DEFAULT_CHANNEL → None.
        """
        media_type = get_media_type(fwd_message)
        fallback = config.DEFAULT_CHANNEL or None
        if media_type == "photo":
            return config.DEFAULT_IMAGE_CHANNEL or fallback
        if media_type in ("video", "animation"):
            return config.DEFAULT_VIDEO_CHANNEL or fallback
        if media_type == "document":
            doc = getattr(fwd_message.media, "document", None)
            mime = getattr(doc, "mime_type", "") or ""
            if mime.startswith("image/"):
                return config.DEFAULT_IMAGE_CHANNEL or fallback
            return config.DEFAULT_VIDEO_CHANNEL or fallback
        return fallback

    # Register the forwarded-message handler last so /commands take priority
    @bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def handle_forwarded_or_target(event):
        if not is_admin(event.sender_id):
            return

        # Skip all slash commands — they are handled by the handlers above
        if event.text and event.text.startswith("/"):
            return

        sender_id = event.sender_id

        # ── Message with media ──────────────────────────────────────────────
        if event.media:
            fwd_message = event.message

            # Albums: buffer all members and process together after a short delay
            if event.message.grouped_id:
                gid = event.message.grouped_id
                _album_buffer.setdefault(gid, []).append(event.message)
                old = _album_timers.get(gid)
                if old:
                    old.cancel()
                _album_timers[gid] = asyncio.create_task(
                    _process_album_after_delay(gid, sender_id)
                )
                return  # don't process individual member

            # Archives: extract and route contents to default channels
            if is_archive(fwd_message):
                if config.DEFAULT_IMAGE_CHANNEL or config.DEFAULT_VIDEO_CHANNEL:
                    status_msg = await event.reply("Processing archive…")
                    try:
                        await _handle_archive(fwd_message, status_msg)
                    except Exception as e:
                        logger.error(f"Archive handling error: {e}", exc_info=True)
                        await status_msg.edit(f"Error: {e}")
                    return
                # No defaults — fall through to ask for target

            # Non-archive media with a matching default channel → auto-route
            default_ch = _default_channel_for(fwd_message)
            if default_ch:
                status_msg = await event.reply("Auto-routing to default channel…")
                try:
                    await _transfer_to_default(fwd_message, default_ch, status_msg)
                except Exception as e:
                    logger.error(f"Auto-route error: {e}", exc_info=True)
                    await status_msg.edit(f"Error: {e}")
                return

            # No default channel configured — store and ask
            pending_forwards[sender_id] = fwd_message
            await event.reply(
                "Where should I send this? Reply with the channel username or URL."
            )
            return

        # ── Plain text message ──────────────────────────────────────────────
        text = (event.text or "").strip()

        # Telegraph URL with no other media → fetch and upload images
        if text and not (sender_id in pending_forwards):
            telegraph_urls = find_telegraph_urls(text)
            if telegraph_urls and config.DEFAULT_IMAGE_CHANNEL:
                status_msg = await event.reply("Processing Telegraph article…")
                try:
                    await _handle_telegraph_urls(telegraph_urls, status_msg)
                except Exception as e:
                    logger.error(f"Telegraph handling error: {e}", exc_info=True)
                    await status_msg.edit(f"Error: {e}")
                return

        # Step 2: plain text reply while a forward is pending — treat as target
        if sender_id in pending_forwards:
            if not text:
                return

            stored = pending_forwards[sender_id]
            status_msg = await event.reply("Resolving target channel...")

            try:
                target_chat = await resolve_chat(userbot, text)
            except Exception as e:
                await status_msg.edit(f"Could not resolve target: {e}\nTry again with a different channel.")
                return  # keep in pending_forwards so user can retry

            # Album path
            if isinstance(stored, list):
                pending_forwards.pop(sender_id, None)
                try:
                    await _transfer_album_to_default(stored, text, status_msg)
                except Exception as e:
                    logger.error(f"Album transfer error: {e}", exc_info=True)
                    await status_msg.edit(f"Error: {e}")
                return

            # Single-message path (existing logic)
            fwd_message = stored
            fwd_from = fwd_message.fwd_from
            channel_id = getattr(fwd_from, "channel_id", None)
            channel_post = getattr(fwd_from, "channel_post", None)

            try:
                if channel_id and channel_post:
                    await status_msg.edit("Fetching original message from source channel...")
                    source_ref = f"-100{channel_id}"
                    try:
                        source_chat = await resolve_chat(userbot, source_ref)
                        messages = await resolve_message(userbot, source_chat, channel_post)
                    except Exception:
                        messages = []

                    if messages:
                        source_url = build_message_url(source_chat, channel_post)
                        await status_msg.edit("Transferring...")
                        if len(messages) == 1:
                            await transfer_one_message(userbot, messages[0], target_chat, source_url, cache=file_cache)
                        else:
                            await transfer_album(userbot, messages, target_chat, source_url, cache=file_cache)
                        pending_forwards.pop(sender_id, None)
                        await status_msg.edit("Done!")
                        return
                    # Fall through to direct download if original not accessible

                # Direct download from the bot's copy of the forwarded message
                await status_msg.edit("Downloading media...")
                media_type = get_media_type(fwd_message)
                buf = io.BytesIO()
                await bot.download_media(fwd_message, file=buf)
                buf.seek(0)
                if hasattr(fwd_message.media, "document"):
                    for attr in getattr(fwd_message.media.document, "attributes", []):
                        if isinstance(attr, DocumentAttributeFilename):
                            buf.name = attr.file_name
                            break
                if not getattr(buf, "name", None):
                    ext = await probe_extension(buf)
                    buf.name = f"file.{ext}"

                await status_msg.edit("Uploading to target channel...")
                if media_type == "photo":
                    await userbot.send_file(target_chat, file=buf, force_document=False)
                elif media_type == "video":
                    video_attr = None
                    if hasattr(fwd_message.media, "document"):
                        for attr in getattr(fwd_message.media.document, "attributes", []):
                            if isinstance(attr, DocumentAttributeVideo):
                                video_attr = attr
                                break
                    duration = getattr(video_attr, "duration", 0) or 0
                    w = getattr(video_attr, "w", 0) or 0
                    h = getattr(video_attr, "h", 0) or 0
                    await userbot.send_file(
                        target_chat, file=buf, force_document=False,
                        attributes=[DocumentAttributeVideo(duration=duration, w=w, h=h, supports_streaming=True)],
                    )
                elif media_type == "voice":
                    await userbot.send_file(target_chat, file=buf, voice_note=True)
                elif media_type == "video_note":
                    await userbot.send_file(target_chat, file=buf, video_note=True)
                else:
                    await userbot.send_file(target_chat, file=buf, force_document=True)
                pending_forwards.pop(sender_id, None)
                await status_msg.edit("Done!")

            except Exception as e:
                logger.error(f"Forward transfer error: {e}", exc_info=True)
                await status_msg.edit(f"Error: {e}\nTry again with a different channel.")


def _parse_message_url(url: str):
    """Parse a t.me message URL into (chat_ref, msg_id)."""
    try:
        url = url.strip("/")
        if "t.me/c/" in url:
            parts = url.split("t.me/c/")[1].split("/")
            return f"-100{parts[0]}", int(parts[1])
        elif "t.me/" in url:
            parts = url.split("t.me/")[1].split("/")
            return parts[0], int(parts[1])
    except (IndexError, ValueError):
        pass
    return None, None


def _format_job(job) -> str:
    pct = f"{job.transferred / job.total * 100:.1f}%" if job.total else "0%"
    text = (
        f"**Job #{job.id}**\n"
        f"Status: {job.status.value}\n"
        f"Progress: {job.transferred}/{job.total} ({pct})\n"
        f"Source: {job.source_chat}\n"
        f"Target: {job.target_chat}\n"
        f"Failed: {len(job.failed_ids)}\n"
        f"Updated: {job.updated_at}"
    )
    if job.status == TransferStatus.INTERRUPTED:
        text += f"\nUse `/resume {job.id}` to continue."
    return text
