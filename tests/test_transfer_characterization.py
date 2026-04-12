import asyncio
import shutil
from pathlib import Path
from types import SimpleNamespace

from telethon.tl.types import DocumentAttributeFilename

import bot as bot_module
import cache as cache_module
import transfer


class CleanupTracker:
    def __init__(self, path: Path):
        self.path = path
        self.cleaned = False

    def cleanup(self):
        self.cleaned = True
        self.path.unlink(missing_ok=True)


class DummyTask:
    def __init__(self, coro):
        self.coro = coro
        self.cancelled = False

    def cancel(self):
        self.cancelled = True
        self.coro.close()


def run(coro):
    return asyncio.run(coro)


def handler(client, name):
    return client.handlers[name]


def make_document_message(make_message, *, message_id=1, text="caption", filename="file.bin", fwd_from=None):
    document = SimpleNamespace(id=message_id * 10, attributes=[DocumentAttributeFilename(file_name=filename)])
    media = SimpleNamespace(document=document)
    return make_message(
        id=message_id,
        text=text,
        message=text,
        media=media,
        has_media=True,
        fwd_from=fwd_from,
    )


def test_transfer_characterization_scaffold_imports(fake_client, fake_source_message):
    assert fake_client is not None
    assert fake_source_message.has_media is True


def test_resolve_message_limits_album_lookup_to_plus_minus_15_ids(make_message):
    grouped = 777
    anchor = make_message(id=100, grouped_id=grouped)
    surrounding = [
        make_message(id=110, grouped_id=grouped),
        make_message(id=97, grouped_id=grouped),
        make_message(id=100, grouped_id=grouped),
        make_message(id=114, grouped_id=999),
        None,
    ]
    calls = []

    class Userbot:
        async def get_messages(self, chat, ids=None, min_id=None, max_id=None):
            calls.append({"chat": chat, "ids": ids, "min_id": min_id, "max_id": max_id})
            if ids is not None:
                return anchor
            return surrounding

    messages = run(transfer.resolve_message(Userbot(), "source-chat", 100))

    assert [message.id for message in messages] == [97, 100, 110]
    assert calls == [
        {"chat": "source-chat", "ids": 100, "min_id": None, "max_id": None},
        {"chat": "source-chat", "ids": None, "min_id": 85, "max_id": 115},
    ]


def test_transfer_one_message_sends_text_with_source_caption(make_message, fake_client, monkeypatch):
    message = make_message(id=5, text="hello world", message="hello world", entities=["entity"])
    telegraph_calls = []

    async def fake_expand(_userbot, text, target_chat):
        telegraph_calls.append((text, target_chat))

    monkeypatch.setattr(transfer, "_expand_telegraph", fake_expand)

    sent = run(transfer.transfer_one_message(fake_client, message, "target-chat", "https://t.me/source/5"))

    assert sent.text == "hello world\n\nSource: https://t.me/source/5"
    assert fake_client.sent_messages == [
        ("target-chat", "hello world\n\nSource: https://t.me/source/5", {"message": "hello world\n\nSource: https://t.me/source/5", "formatting_entities": ["entity"]})
    ]
    assert telegraph_calls == [("hello world", "target-chat")]


def test_transfer_one_message_document_upload_uses_force_document_and_cleans_up(tmp_path, make_message, fake_client, monkeypatch):
    source_file = tmp_path / "upload.bin"
    source_file.write_bytes(b"payload")
    tracker = CleanupTracker(source_file)
    telegraph_calls = []
    message = make_document_message(make_message, message_id=7, text="doc caption", filename="archive.bin")

    async def fake_download(*_args, **_kwargs):
        return tracker

    async def fake_expand(_userbot, text, target_chat):
        telegraph_calls.append((text, target_chat))

    monkeypatch.setattr(transfer, "_download_to_file", fake_download)
    monkeypatch.setattr(transfer, "get_media_type", lambda _message: "document")
    monkeypatch.setattr(transfer, "_expand_telegraph", fake_expand)

    sent = run(transfer.transfer_one_message(fake_client, message, "target-chat", "source-url"))

    assert sent == source_file
    assert fake_client.sent_files == [
        (
            "target-chat",
            {
                "file": source_file,
                "caption": "doc caption\n\nSource: source-url",
                "formatting_entities": [],
                "force_document": True,
                "progress_callback": fake_client.sent_files[0][1]["progress_callback"],
            },
        )
    ]
    assert tracker.cleaned is True
    assert source_file.exists() is False
    assert telegraph_calls == [("doc caption", "target-chat")]


def test_transfer_album_skips_failed_downloads_and_puts_source_only_on_last_valid_caption(tmp_path, make_message, fake_client, monkeypatch):
    first = make_document_message(make_message, message_id=1, text="first")
    second = make_document_message(make_message, message_id=2, text="second")
    third = make_document_message(make_message, message_id=3, text="third")
    first_file = tmp_path / "first.bin"
    third_file = tmp_path / "third.bin"
    first_file.write_bytes(b"1")
    third_file.write_bytes(b"3")
    first_tracker = CleanupTracker(first_file)
    third_tracker = CleanupTracker(third_file)

    async def fake_download(_userbot, message, _cache):
        if message.id == 1:
            return first_tracker
        if message.id == 2:
            raise RuntimeError("boom")
        return third_tracker

    monkeypatch.setattr(transfer, "_download_to_file", fake_download)

    sent = run(transfer.transfer_album(fake_client, [first, second, third], "album-chat", "source-url"))

    upload = fake_client.sent_files[0]
    assert sent == [first_file, third_file]
    assert upload[0] == "album-chat"
    assert upload[1]["file"] == [first_file, third_file]
    assert upload[1]["caption"] == ["first", "third\n\nSource: source-url"]
    assert first_tracker.cleaned is True
    assert third_tracker.cleaned is True
    assert first_file.exists() is False
    assert third_file.exists() is False


def test_upload_downloaded_message_without_source_url_preserves_caption_and_document_mode(tmp_path, make_message, fake_client, monkeypatch):
    source_file = tmp_path / "upload.bin"
    source_file.write_bytes(b"payload")
    tracker = CleanupTracker(source_file)
    message = make_document_message(make_message, message_id=70, text="doc caption", filename="archive.bin")

    monkeypatch.setattr(transfer, "get_media_type", lambda _message: "document")

    sent = run(transfer.upload_downloaded_message(fake_client, message, tracker, "target-chat"))

    assert sent == source_file
    assert fake_client.sent_files == [
        (
            "target-chat",
            {
                "file": source_file,
                "caption": "doc caption",
                "formatting_entities": [],
                "force_document": True,
                "progress_callback": None,
            },
        )
    ]
    assert source_file.exists() is True


def test_upload_downloaded_album_without_source_url_preserves_member_captions_and_force_document_override(tmp_path, make_message, fake_client):
    first = make_document_message(make_message, message_id=71, text="first")
    second = make_document_message(make_message, message_id=72, text="second")
    first_file = tmp_path / "first.bin"
    second_file = tmp_path / "second.bin"
    first_file.write_bytes(b"1")
    second_file.write_bytes(b"2")

    sent = run(
        transfer.upload_downloaded_album(
            fake_client,
            [first, second],
            [CleanupTracker(first_file), CleanupTracker(second_file)],
            "album-chat",
            force_document=False,
        )
    )

    assert sent == [first_file, second_file]
    assert fake_client.sent_files == [
        (
            "album-chat",
            {
                "file": [first_file, second_file],
                "caption": ["first", "second"],
                "progress_callback": None,
                "force_document": False,
            },
        )
    ]


def test_download_to_file_cache_hit_copies_into_temp_dir_with_message_extension(tmp_path, monkeypatch, make_message):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cached_path = cache_dir / "doc_10"
    cached_path.write_bytes(b"cached bytes")
    temp_dir = tmp_path / "temp"
    copied = []
    real_copy2 = shutil.copy2
    message = make_document_message(make_message, message_id=1, filename="report.txt")

    class Cache:
        def get(self, file_id):
            assert file_id == "doc_10"
            return cached_path

    def fake_copy(src, dst):
        copied.append((src, dst))
        return real_copy2(src, dst)

    monkeypatch.setattr(transfer.config, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(transfer.os, "link", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("no link")))
    monkeypatch.setattr(transfer.shutil, "copy2", fake_copy)

    media_file = run(transfer._download_to_file(object(), message, cache=Cache()))

    assert media_file.owned is True
    assert media_file.path.suffix == ".txt"
    assert media_file.path.read_bytes() == b"cached bytes"
    assert copied == [(cached_path, media_file.path)]

    media_file.cleanup()
    assert media_file.path.exists() is False
    assert cached_path.exists() is True


def test_download_to_file_keeps_bin_path_without_self_rename(tmp_path, monkeypatch, make_message):
    temp_dir = tmp_path / "temp"
    message = make_message(id=586, text="photo", message="photo", media=SimpleNamespace(photo=SimpleNamespace(id=5860)), has_media=True)
    observed = {}

    class Client:
        async def download_media(self, _message, *, file, progress_callback=None):
            path = Path(file)
            path.write_bytes(b"downloaded")
            observed["path"] = path
            observed["progress_callback"] = progress_callback
            return str(path)

    monkeypatch.setattr(transfer.config, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(transfer, "_get_doc_ext", lambda _message: "")
    monkeypatch.setattr(transfer, "probe_extension_file", lambda _path: asyncio.sleep(0, result="bin"))

    media_file = run(transfer._download_to_file(Client(), message))

    assert media_file is not None
    assert media_file.path == observed["path"]
    assert media_file.path.suffix == ".bin"
    assert media_file.path.read_bytes() == b"downloaded"
    media_file.cleanup()
    assert media_file.path.exists() is False


def test_download_to_file_uses_actual_download_media_return_path(tmp_path, monkeypatch, make_message):
    temp_dir = tmp_path / "temp"
    actual_path = temp_dir / "telegram-photo.jpg"
    message = make_message(id=587, text="photo", message="photo", media=SimpleNamespace(photo=SimpleNamespace(id=5870)), has_media=True)

    class Client:
        async def download_media(self, _message, *, file, progress_callback=None):
            Path(file).parent.mkdir(parents=True, exist_ok=True)
            actual_path.write_bytes(b"jpeg-bytes")
            return str(actual_path)

    monkeypatch.setattr(transfer.config, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(transfer, "_get_doc_ext", lambda _message: "")
    monkeypatch.setattr(transfer, "probe_extension_file", lambda _path: asyncio.sleep(0, result="jpg"))

    media_file = run(transfer._download_to_file(Client(), message))

    assert media_file is not None
    assert media_file.path == actual_path
    assert media_file.path.read_bytes() == b"jpeg-bytes"
    media_file.cleanup()
    assert media_file.path.exists() is False


def test_download_to_file_forced_message_download_uses_download_media_for_documents(tmp_path, monkeypatch, make_message):
    temp_dir = tmp_path / "temp"
    actual_path = temp_dir / "telegram-document.bin"
    message = make_document_message(make_message, message_id=589, filename="report.bin")
    calls = []

    class Client:
        async def download_media(self, _message, *, file, progress_callback=None):
            calls.append(("download_media", Path(file), progress_callback))
            Path(file).parent.mkdir(parents=True, exist_ok=True)
            actual_path.write_bytes(b"doc-bytes")
            return str(actual_path)

    monkeypatch.setattr(transfer.config, "TEMP_DIR", temp_dir)

    media_file = run(
        transfer._download_to_file(
            Client(),
            message,
            force_message_download=True,
        )
    )

    assert media_file is not None
    assert media_file.path == actual_path
    assert media_file.path.read_bytes() == b"doc-bytes"
    assert len(calls) == 1
    media_file.cleanup()
    assert media_file.path.exists() is False


def test_public_download_to_file_raises_readable_error_and_cleans_partial_file(tmp_path, monkeypatch, make_message):
    temp_dir = tmp_path / "temp"
    message = make_message(id=588, text="photo", message="photo", media=SimpleNamespace(photo=SimpleNamespace(id=5880)), has_media=True)
    observed = {}

    class Client:
        async def download_media(self, _message, *, file, progress_callback=None):
            path = Path(file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"partial")
            observed["path"] = path
            return str(path.parent / "missing.bin")

    monkeypatch.setattr(transfer.config, "TEMP_DIR", temp_dir)

    try:
        run(transfer.download_to_file(Client(), message))
        raise AssertionError("download_to_file should have raised MediaDownloadError")
    except transfer.MediaDownloadError as exc:
        assert exc.message_id == 588
        assert exc.phase == "verifying downloaded file"
        assert "Media download failed for message 588" in str(exc)
        assert "missing.bin" in str(exc)

    assert observed["path"].exists() is False


def test_file_cache_put_file_falls_back_to_copy_and_get_touches_atime(tmp_path, monkeypatch):
    source = tmp_path / "source.bin"
    source.write_bytes(b"cache me")
    touched = []
    copied = []
    real_copy2 = shutil.copy2
    file_cache = cache_module.FileCache(str(tmp_path / "cache"), max_size_bytes=1024)

    monkeypatch.setattr(cache_module.os, "link", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("no link")))

    def fake_copy(src, dst):
        copied.append((Path(src), Path(dst)))
        return real_copy2(src, dst)

    monkeypatch.setattr(cache_module.shutil, "copy2", fake_copy)
    monkeypatch.setattr(
        cache_module.os,
        "utime",
        lambda path, *args, **kwargs: touched.append(Path(path)),
    )

    stored = run(file_cache.put_file("media-key", source))
    fetched = file_cache.get("media-key")

    assert stored.read_bytes() == b"cache me"
    assert copied == [(source, stored)]
    assert fetched == stored
    assert touched == [stored, stored]


def test_auto_route_single_forward_falls_back_silently_to_direct_download(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    fwd_message = make_document_message(
        make_message,
        message_id=11,
        text="forwarded",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=55),
    )
    event = fake_event(sender_id=42, message=fwd_message)
    direct_file = tmp_path / "forward.bin"
    direct_file.write_bytes(b"forward")
    tracker = CleanupTracker(direct_file)

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")
    monkeypatch.setattr(bot_module, "get_media_type", lambda _message: "photo")
    monkeypatch.setattr(bot_module, "resolve_chat", lambda _client, ref: _resolve_chat(ref))

    async def _resolve_chat(ref):
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise AssertionError("source lookup should not run when bot download succeeds")

    async def fake_download(*_args, **_kwargs):
        return tracker

    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert status.text == "Done!"
    assert status.edits == [
        "Downloading media...",
        "Uploading...",
        "Done!",
    ]
    assert userbot.sent_files[0][0] == "target-chat"
    assert userbot.sent_files[0][1]["file"] == direct_file
    assert userbot.sent_files[0][1]["force_document"] is False
    assert tracker.cleaned is True


def test_auto_route_album_fallback_uploads_grouped_files_with_force_document_false(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    tasks = []
    first = make_document_message(
        make_message,
        message_id=21,
        text="one",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=88),
    )
    second = make_document_message(
        make_message,
        message_id=22,
        text="two",
        fwd_from=SimpleNamespace(channel_id=777, channel_post=88),
    )
    first.grouped_id = 555
    second.grouped_id = 555
    first_file = tmp_path / "album-1.bin"
    second_file = tmp_path / "album-2.bin"
    first_file.write_bytes(b"1")
    second_file.write_bytes(b"2")
    trackers = {21: CleanupTracker(first_file), 22: CleanupTracker(second_file)}

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_CHANNEL", "@default")
    monkeypatch.setattr(bot_module, "get_media_type", lambda _message: "photo")
    monkeypatch.setattr(bot_module.asyncio, "sleep", lambda _delay: _no_op())
    monkeypatch.setattr(bot_module.asyncio, "create_task", lambda coro: _capture_task(coro, tasks))
    monkeypatch.setattr(bot_module, "resolve_chat", lambda _client, ref: _resolve_chat(ref))

    async def _resolve_chat(ref):
        return {"@default": "target-chat", "-100777": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise AssertionError("album source lookup should not run when bot downloads succeed")

    async def fake_download(*_args, **_kwargs):
        return trackers[_args[1].id]

    async def _no_op():
        return None

    def _capture_task(coro, created_tasks):
        task = DummyTask(coro)
        created_tasks.append(task)
        return task

    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(fake_event(sender_id=9, message=first)))
    run(handle(fake_event(sender_id=9, message=second)))
    run(tasks[-1].coro)

    status = bot_client.bot_messages[0][1]
    assert status.edits == [
        "Downloading 2 media file(s)...",
        "Uploading album...",
        "Done!",
    ]
    assert userbot.sent_files[0][0] == "target-chat"
    assert userbot.sent_files[0][1]["file"] == [first_file, second_file]
    assert userbot.sent_files[0][1]["force_document"] is False
    assert trackers[21].cleaned is True
    assert trackers[22].cleaned is True


def test_pending_album_list_is_popped_before_transfer_attempt(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    stored_album = [make_document_message(make_message, message_id=31), make_document_message(make_message, message_id=32)]
    event = fake_event(sender_id=50, text="@target", message=make_message(id=999, text="@target", message="@target"))
    bot_module.pending_forwards[50] = stored_album

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)

    async def fake_resolve_chat(_client, _ref):
        return "target-chat"

    async def fake_download(*_args, **_kwargs):
        raise RuntimeError("album fallback blew up")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    assert 50 not in bot_module.pending_forwards
    assert event.replies[0].edits == [
        "Downloading 2 media file(s)...",
        "Error: album fallback blew up",
    ]


def test_pending_single_forward_falls_back_to_direct_download_and_clears_pending(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    fwd_message = make_document_message(
        make_message,
        message_id=41,
        fwd_from=SimpleNamespace(channel_id=888, channel_post=99),
    )
    event = fake_event(sender_id=77, text="@target", message=make_message(id=400, text="@target", message="@target"))
    bot_module.pending_forwards[77] = fwd_message
    direct_file = tmp_path / "pending.bin"
    direct_file.write_bytes(b"pending")
    tracker = CleanupTracker(direct_file)

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache="cache-sentinel")
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module, "get_media_type", lambda _message: "document")

    async def fake_resolve_chat(_client, ref):
        return {"@target": "target-chat", "-100888": "source-chat"}[ref]

    async def fake_resolve_message(*_args, **_kwargs):
        raise AssertionError("source lookup should not run when bot download succeeds")

    async def fake_download(*_args, **_kwargs):
        return tracker

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "resolve_message", fake_resolve_message)
    monkeypatch.setattr(bot_module, "download_to_file", fake_download)

    run(handle(event))

    status = event.replies[0]
    assert 77 not in bot_module.pending_forwards
    assert status.edits == [
        "Downloading media...",
        "Uploading...",
        "Done!",
    ]
    assert userbot.sent_files[0][0] == "target-chat"
    assert userbot.sent_files[0][1]["force_document"] is True
    assert tracker.cleaned is True
