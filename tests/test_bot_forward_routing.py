import asyncio
from pathlib import Path
from types import SimpleNamespace

import bot as bot_module

from tests.test_transfer_characterization import CleanupTracker, DummyTask, handler, make_document_message, run


def test_auto_route_single_forward_uses_shared_upload_helper_after_source_fetch_fails(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    fwd_message = make_document_message(
        make_message,
        message_id=101,
        text="forwarded caption",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=55),
    )
    event = fake_event(sender_id=42, message=fwd_message)
    direct_file = tmp_path / "forward.bin"
    direct_file.write_bytes(b"forward")
    tracker = CleanupTracker(direct_file)
    uploads = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")

    async def fake_resolve_chat(_client, ref):
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise RuntimeError("source fetch failed")

    async def fake_download(*_args, **_kwargs):
        return tracker

    async def fake_upload(userbot_client, message, mf, target_chat, source_url=None, **_kwargs):
        uploads.append((userbot_client, message, mf.path, target_chat, source_url))
        return "uploaded"

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_message", fake_upload)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Fetching original message from source channel...",
        "Downloading media...",
        "Uploading...",
        "Done!",
    ]
    assert uploads == [(userbot, fwd_message, direct_file, "target-chat", None)]
    assert tracker.cleaned is True


def test_auto_route_album_uses_shared_upload_helper_after_source_lookup_misses(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    tasks = []
    first = make_document_message(
        make_message,
        message_id=121,
        text="one",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=88),
    )
    second = make_document_message(
        make_message,
        message_id=122,
        text="two",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=88),
    )
    first.grouped_id = 555
    second.grouped_id = 555
    first_file = tmp_path / "album-1.bin"
    second_file = tmp_path / "album-2.bin"
    first_file.write_bytes(b"1")
    second_file.write_bytes(b"2")
    trackers = {121: CleanupTracker(first_file), 122: CleanupTracker(second_file)}
    uploads = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")
    monkeypatch.setattr(bot_module.asyncio, "sleep", lambda _delay: _no_op())
    monkeypatch.setattr(bot_module.asyncio, "create_task", lambda coro: _capture_task(coro, tasks))

    async def fake_resolve_chat(_client, ref):
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        return []

    async def fake_download(*_args, **_kwargs):
        return trackers[_args[1].id]

    async def fake_upload(userbot_client, messages, media_files, target_chat, source_url=None, force_document=None, **_kwargs):
        uploads.append((userbot_client, messages, [mf.path for mf in media_files], target_chat, source_url, force_document))
        return ["uploaded"]

    async def _no_op():
        return None

    def _capture_task(coro, created_tasks):
        task = DummyTask(coro)
        created_tasks.append(task)
        return task

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_album", fake_upload)

    run(handle(fake_event(sender_id=9, message=first)))
    run(handle(fake_event(sender_id=9, message=second)))
    run(tasks[-1].coro)

    status = bot_client.bot_messages[0][1]
    assert status.edits == [
        "Fetching original album from source channel...",
        "Downloading 2 media file(s)...",
        "Uploading album...",
        "Done!",
    ]
    assert uploads == [
        (userbot, [first, second], [first_file, second_file], "target-chat", None, False)
    ]
    assert trackers[121].cleaned is True
    assert trackers[122].cleaned is True


def test_pending_single_forward_uses_shared_upload_helper_and_clears_pending(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    fwd_message = make_document_message(
        make_message,
        message_id=141,
        text="pending caption",
        fwd_from=SimpleNamespace(channel_id=888, channel_post=99),
    )
    event = fake_event(sender_id=77, text="@target", message=make_message(id=400, text="@target", message="@target"))
    bot_module.pending_forwards[77] = fwd_message
    direct_file = tmp_path / "pending.bin"
    direct_file.write_bytes(b"pending")
    tracker = CleanupTracker(direct_file)
    uploads = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)

    async def fake_resolve_chat(_client, ref):
        return {"@target": "target-chat", "-100888": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise RuntimeError("source fetch failed")

    async def fake_download(*_args, **_kwargs):
        return tracker

    async def fake_upload(userbot_client, message, mf, target_chat, source_url=None, **_kwargs):
        uploads.append((userbot_client, message, mf.path, target_chat, source_url))
        return "uploaded"

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_message", fake_upload)

    run(handle(event))

    status = event.replies[0]
    assert 77 not in bot_module.pending_forwards
    assert status.edits == [
        "Fetching original message from source channel...",
        "Downloading media...",
        "Uploading...",
        "Done!",
    ]
    assert uploads == [(userbot, fwd_message, direct_file, "target-chat", None)]
    assert tracker.cleaned is True
