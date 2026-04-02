import asyncio
from types import SimpleNamespace

import bot as bot_module

from tests.test_transfer_characterization import CleanupTracker, DummyTask, handler, make_document_message, run


async def _no_op():
    return None


def _capture_task(coro, created_tasks):
    task = DummyTask(coro)
    created_tasks.append(task)
    return task


def test_auto_route_single_forward_prefers_bot_download_before_any_userbot_lookup(tmp_path, fake_client, fake_event, make_message, monkeypatch):
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
    calls = []
    uploads = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise AssertionError("source lookup should not run when bot download succeeds")

    async def fake_download(*_args, **_kwargs):
        calls.append(("download_to_file", _args[1].id))
        return tracker

    async def fake_upload(userbot_client, message, mf, target_chat, source_url=None, **_kwargs):
        calls.append(("upload_downloaded_message", message.id, target_chat, source_url))
        uploads.append((userbot_client, message, mf.path, target_chat, source_url))
        return "uploaded"

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_message", fake_upload)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Downloading media...",
        "Uploading...",
        "Done!",
    ]
    assert calls == [
        ("resolve_chat", "@default"),
        ("download_to_file", 101),
        ("upload_downloaded_message", 101, "target-chat", None),
    ]
    assert uploads == [(userbot, fwd_message, direct_file, "target-chat", None)]
    assert tracker.cleaned is True


def test_auto_route_album_prefers_bot_downloads_before_any_userbot_lookup(tmp_path, fake_client, fake_event, make_message, monkeypatch):
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
    calls = []
    uploads = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")
    monkeypatch.setattr(bot_module.asyncio, "sleep", lambda _delay: _no_op())
    monkeypatch.setattr(bot_module.asyncio, "create_task", lambda coro: _capture_task(coro, tasks))

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise AssertionError("album source lookup should not run when bot downloads succeed")

    async def fake_download(*_args, **_kwargs):
        calls.append(("download_to_file", _args[1].id))
        return trackers[_args[1].id]

    async def fake_upload(userbot_client, messages, media_files, target_chat, source_url=None, force_document=None, **_kwargs):
        calls.append(("upload_downloaded_album", [msg.id for msg in messages], target_chat, source_url, force_document))
        uploads.append((userbot_client, messages, [mf.path for mf in media_files], target_chat, source_url, force_document))
        return ["uploaded"]

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_album", fake_upload)

    run(handle(fake_event(sender_id=9, message=first)))
    run(handle(fake_event(sender_id=9, message=second)))
    run(tasks[-1].coro)

    status = bot_client.bot_messages[0][1]
    assert status.edits == [
        "Downloading 2 media file(s)...",
        "Uploading album...",
        "Done!",
    ]
    assert calls == [
        ("resolve_chat", "@default"),
        ("download_to_file", 121),
        ("download_to_file", 122),
        ("upload_downloaded_album", [121, 122], "target-chat", None, False),
    ]
    assert uploads == [
        (userbot, [first, second], [first_file, second_file], "target-chat", None, False)
    ]
    assert trackers[121].cleaned is True
    assert trackers[122].cleaned is True


def test_pending_single_forward_falls_back_to_userbot_only_after_bot_download_fails(fake_client, fake_event, make_message, monkeypatch):
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
    source_message = make_document_message(make_message, message_id=99, text="source caption")
    calls = []
    transfers = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module, "build_message_url", lambda chat, msg_id: f"url:{chat}:{msg_id}")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"@target": "target-chat", "-100888": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        calls.append(("resolve_message", _args[2]))
        return [source_message]

    async def fake_download(*_args, **_kwargs):
        calls.append(("download_to_file", _args[1].id))
        raise RuntimeError("bot download failed")

    async def fake_transfer_one_message(userbot_client, message, target_chat, source_url=None, **_kwargs):
        calls.append(("transfer_one_message", message.id, target_chat, source_url))
        transfers.append((userbot_client, message.id, target_chat, source_url))
        return "uploaded"

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "transfer_one_message", fake_transfer_one_message)

    run(handle(event))

    status = event.replies[0]
    assert 77 not in bot_module.pending_forwards
    assert status.edits == [
        "Downloading media...",
        "Fetching original message from source channel...",
        "Transferring...",
        "Done!",
    ]
    assert calls == [
        ("resolve_chat", "@target"),
        ("download_to_file", 141),
        ("resolve_chat", "-100888"),
        ("resolve_message", 99),
        ("transfer_one_message", 99, "target-chat", "url:source-chat:99"),
    ]
    assert transfers == [(userbot, 99, "target-chat", "url:source-chat:99")]


def test_auto_route_album_falls_back_as_a_whole_after_bot_download_failure(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    tasks = []
    first = make_document_message(
        make_message,
        message_id=151,
        text="one",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=88),
    )
    second = make_document_message(
        make_message,
        message_id=152,
        text="two",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=88),
    )
    first.grouped_id = 999
    second.grouped_id = 999
    source_first = make_document_message(make_message, message_id=88, text="src-one")
    source_second = make_document_message(make_message, message_id=89, text="src-two")
    calls = []
    transfers = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")
    monkeypatch.setattr(bot_module, "build_message_url", lambda chat, msg_id: f"url:{chat}:{msg_id}")
    monkeypatch.setattr(bot_module.asyncio, "sleep", lambda _delay: _no_op())
    monkeypatch.setattr(bot_module.asyncio, "create_task", lambda coro: _capture_task(coro, tasks))

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        calls.append(("resolve_message", _args[2]))
        return [source_first, source_second]

    async def fake_download(*_args, **_kwargs):
        calls.append(("download_to_file", _args[1].id))
        if _args[1].id == 151:
            raise RuntimeError("album member failed")
        raise AssertionError("bot download path should stop after first album failure")

    async def fake_transfer_album(userbot_client, messages, target_chat, source_url=None, **_kwargs):
        calls.append(("transfer_album", [msg.id for msg in messages], target_chat, source_url))
        transfers.append((userbot_client, [msg.id for msg in messages], target_chat, source_url))
        return ["uploaded"]

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "transfer_album", fake_transfer_album)

    run(handle(fake_event(sender_id=11, message=first)))
    run(handle(fake_event(sender_id=11, message=second)))
    run(tasks[-1].coro)

    status = bot_client.bot_messages[0][1]
    assert status.edits == [
        "Downloading 2 media file(s)...",
        "Fetching original album from source channel...",
        "Transferring album...",
        "Done!",
    ]
    assert calls == [
        ("resolve_chat", "@default"),
        ("download_to_file", 151),
        ("resolve_chat", "-100777"),
        ("resolve_message", 88),
        ("transfer_album", [88, 89], "target-chat", "url:source-chat:88"),
    ]
    assert transfers == [(userbot, [88, 89], "target-chat", "url:source-chat:88")]


def test_cmd_transfer_keeps_userbot_fetch_for_explicit_message_url(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(sender_id=17, message=make_message(id=501, text="/transfer", message="/transfer"))
    event.pattern_match = SimpleNamespace(group=lambda _index: "https://t.me/sourcechan/55 @target")
    source_message = make_document_message(make_message, message_id=55, text="hello")
    calls = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "cmd_transfer")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module, "build_message_url", lambda chat, msg_id: f"url:{chat}:{msg_id}")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"sourcechan": "source-chat", "@target": "target-chat"}[ref]

    async def fake_resolve_message(_client, chat, msg_id):
        calls.append(("resolve_message", chat, msg_id))
        return [source_message]

    async def fake_transfer_one_message(_client, message, target_chat, source_url=None, **_kwargs):
        calls.append(("transfer_one_message", message.id, target_chat, source_url))
        return "uploaded"

    async def fake_download(*_args, **_kwargs):
        raise AssertionError("explicit message URLs should not use forwarded bot-download helpers")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "transfer_one_message", fake_transfer_one_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Fetching message...",
        "Transferring...",
        "Done! Transferred message 55.",
    ]
    assert calls == [
        ("resolve_chat", "sourcechan"),
        ("resolve_chat", "@target"),
        ("resolve_message", "source-chat", 55),
        ("transfer_one_message", 55, "target-chat", "url:source-chat:55"),
    ]


def test_cmd_transfer_keeps_userbot_fetch_for_private_message_url(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(sender_id=170, message=make_message(id=510, text="/transfer", message="/transfer"))
    event.pattern_match = SimpleNamespace(group=lambda _index: "https://t.me/c/1234567890/55 @target")
    source_message = make_document_message(make_message, message_id=55, text="hello")
    calls = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "cmd_transfer")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module, "build_message_url", lambda chat, msg_id: f"url:{chat}:{msg_id}")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"-1001234567890": "private-source-chat", "@target": "target-chat"}[ref]

    async def fake_resolve_message(_client, chat, msg_id):
        calls.append(("resolve_message", chat, msg_id))
        return [source_message]

    async def fake_transfer_one_message(_client, message, target_chat, source_url=None, **_kwargs):
        calls.append(("transfer_one_message", message.id, target_chat, source_url))
        return "uploaded"

    async def fake_download(*_args, **_kwargs):
        raise AssertionError("private message URLs should stay on the userbot path")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "transfer_one_message", fake_transfer_one_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Fetching message...",
        "Transferring...",
        "Done! Transferred message 55.",
    ]
    assert calls == [
        ("resolve_chat", "-1001234567890"),
        ("resolve_chat", "@target"),
        ("resolve_message", "private-source-chat", 55),
        ("transfer_one_message", 55, "target-chat", "url:private-source-chat:55"),
    ]


def test_cmd_transfer_parse_failure_does_not_enter_forwarded_bot_download_path(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(sender_id=18, message=make_message(id=502, text="/transfer", message="/transfer"))
    event.pattern_match = SimpleNamespace(group=lambda _index: "not-a-url @target")

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "cmd_transfer")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)

    async def fake_download(*_args, **_kwargs):
        raise AssertionError("malformed transfer URLs should not reach bot download helpers")

    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == ["Could not parse message URL."]


def test_private_message_link_flow_keeps_userbot_fetch_and_skips_forwarded_bot_download(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(
        sender_id=19,
        text="check https://t.me/sourcechan/77",
        message=make_message(id=503, text="check https://t.me/sourcechan/77", message="check https://t.me/sourcechan/77"),
    )
    source_message = make_document_message(make_message, message_id=77, text="linked")
    calls = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module, "build_message_url", lambda chat, msg_id: f"url:{chat}:{msg_id}")
    monkeypatch.setattr(bot_module, "find_message_urls", lambda _text: [("sourcechan", 77)])
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"sourcechan": "source-chat", "@default": "target-chat"}[ref]

    async def fake_resolve_message(_client, chat, msg_id):
        calls.append(("resolve_message", chat, msg_id))
        return [source_message]

    async def fake_transfer_one_message(_client, message, target_chat, source_url=None, **_kwargs):
        calls.append(("transfer_one_message", message.id, target_chat, source_url))
        return "uploaded"

    async def fake_download(*_args, **_kwargs):
        raise AssertionError("message-link flow should remain userbot-driven")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "transfer_one_message", fake_transfer_one_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Transferring…",
        "Done!",
    ]
    assert calls == [
        ("resolve_chat", "sourcechan"),
        ("resolve_message", "source-chat", 77),
        ("resolve_chat", "@default"),
        ("transfer_one_message", 77, "target-chat", "url:source-chat:77"),
    ]


def test_private_c_message_link_flow_keeps_userbot_fetch_and_skips_forwarded_bot_download(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(
        sender_id=191,
        text="check https://t.me/c/1234567890/77",
        message=make_message(id=504, text="check https://t.me/c/1234567890/77", message="check https://t.me/c/1234567890/77"),
    )
    source_message = make_document_message(make_message, message_id=77, text="linked-private")
    calls = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module, "build_message_url", lambda chat, msg_id: f"url:{chat}:{msg_id}")
    monkeypatch.setattr(bot_module, "find_message_urls", lambda _text: [("-1001234567890", 77)])
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"-1001234567890": "private-source-chat", "@default": "target-chat"}[ref]

    async def fake_resolve_message(_client, chat, msg_id):
        calls.append(("resolve_message", chat, msg_id))
        return [source_message]

    async def fake_transfer_one_message(_client, message, target_chat, source_url=None, **_kwargs):
        calls.append(("transfer_one_message", message.id, target_chat, source_url))
        return "uploaded"

    async def fake_download(*_args, **_kwargs):
        raise AssertionError("private text message-link flow should remain userbot-driven")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "transfer_one_message", fake_transfer_one_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Transferring…",
        "Done!",
    ]
    assert calls == [
        ("resolve_chat", "-1001234567890"),
        ("resolve_message", "private-source-chat", 77),
        ("resolve_chat", "@default"),
        ("transfer_one_message", 77, "target-chat", "url:private-source-chat:77"),
    ]


def test_single_forward_upload_failure_does_not_fallback_to_userbot_source(fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    fwd_message = make_document_message(
        make_message,
        message_id=171,
        text="forwarded caption",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=55),
    )
    event = fake_event(sender_id=42, message=fwd_message)
    calls = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")

    async def fake_resolve_chat(_client, ref):
        calls.append(("resolve_chat", ref))
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise AssertionError("upload failures must not trigger userbot source fallback")

    async def fake_download(*_args, **_kwargs):
        calls.append(("download_to_file", _args[1].id))
        return SimpleNamespace(cleanup=lambda: calls.append(("cleanup", 171)), path="unused")

    async def fake_upload(*_args, **_kwargs):
        calls.append(("upload_downloaded_message", _args[1].id))
        raise RuntimeError("upload exploded")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_message", fake_upload)

    run(handle(event))

    status = event.replies[0]
    assert status.edits == [
        "Downloading media...",
        "Uploading...",
        "Error: upload exploded",
    ]
    assert calls == [
        ("resolve_chat", "@default"),
        ("download_to_file", 171),
        ("upload_downloaded_message", 171),
        ("cleanup", 171),
    ]
