import asyncio
import io
import logging
from typing import Dict, Optional, Tuple

from telethon import TelegramClient, events
from telethon.tl.custom import Button
from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo, Message

import config
import database as db_module
from media import get_media_type
from transfer import resolve_chat, resolve_message, transfer_one_message, transfer_album, transfer_bulk, transfer_bulk_files
from utils import build_message_url

logger = logging.getLogger(__name__)

# job_id -> (asyncio.Task, asyncio.Event)
active_tasks: Dict[int, Tuple[asyncio.Task, asyncio.Event]] = {}

# user_id -> forwarded Message awaiting a target channel
pending_forwards: Dict[int, Message] = {}


def is_admin(user_id: int) -> bool:
    return user_id in config.ADMIN_IDS


def register_handlers(bot: TelegramClient, userbot: TelegramClient, db, file_cache=None):

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

        if job_id not in active_tasks:
            return await event.reply(f"Job #{job_id} is not currently running.")

        _, cancel_event = active_tasks[job_id]
        cancel_event.set()
        await event.reply(
            f"Cancellation requested for job #{job_id}. "
            "It will stop after the current transfer completes."
        )

    # Register the forwarded-message handler last so /commands take priority
    @bot.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def handle_forwarded_or_target(event):
        if not is_admin(event.sender_id):
            return

        # Skip all slash commands — they are handled by the handlers above
        if event.text and event.text.startswith("/"):
            return

        sender_id = event.sender_id

        # Step 1: message with media (directly sent or forwarded) — store and ask for target
        if event.media:
            pending_forwards[sender_id] = event.message
            await event.reply(
                "Where should I send this? Reply with the channel username or URL."
            )
            return

        # Step 2: plain text reply while a forward is pending — treat as target
        if sender_id in pending_forwards:
            target_ref = (event.text or "").strip()
            if not target_ref:
                return

            fwd_message = pending_forwards[sender_id]
            status_msg = await event.reply("Resolving target channel...")

            try:
                target_chat = await resolve_chat(userbot, target_ref)
            except Exception as e:
                await status_msg.edit(f"Could not resolve target: {e}\nTry again with a different channel.")
                return  # keep in pending_forwards so user can retry

            # Determine source: if forwarded from a channel, use the original message via userbot
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
    return (
        f"**Job #{job.id}**\n"
        f"Status: {job.status.value}\n"
        f"Progress: {job.transferred}/{job.total} ({pct})\n"
        f"Source: {job.source_chat}\n"
        f"Target: {job.target_chat}\n"
        f"Failed: {len(job.failed_ids)}\n"
        f"Updated: {job.updated_at}"
    )
