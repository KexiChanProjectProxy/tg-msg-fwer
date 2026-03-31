from pathlib import Path

import bot as bot_module

from tests.test_transfer_characterization import handler, make_document_message, run


def test_telegraph_images_use_shared_local_upload_and_cleanup_on_success(
    tmp_path, fake_client, fake_event, make_message, monkeypatch
):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(
        sender_id=42,
        text="see https://telegra.ph/example",
        message=make_message(id=501, text="see https://telegra.ph/example", message="see https://telegra.ph/example"),
    )
    first = tmp_path / "telegraph-1.jpg"
    second = tmp_path / "telegraph-2.jpg"
    first.write_bytes(b"1")
    second.write_bytes(b"2")
    uploads = []

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache=None)
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_IMAGE_CHANNEL", "@images")

    async def fake_resolve_chat(_client, ref):
        assert ref == "@images"
        return "image-chat"

    async def fake_fetch(_url):
        return "Telegraph Title", "body", ["https://telegra.ph/file/a.jpg", "https://telegra.ph/file/b.jpg"]

    async def fake_download(_urls):
        return [first, second]

    async def fake_upload(userbot_client, messages, media_files, target_chat, source_url=None, force_document=None, **_kwargs):
        uploads.append((userbot_client, [m.message for m in messages], [mf.path for mf in media_files], target_chat, source_url, force_document))
        return ["uploaded"]

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "fetch_telegraph_page", fake_fetch)
    monkeypatch.setattr(bot_module, "download_telegraph_images", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_album", fake_upload)

    run(handle(event))

    assert event.replies[0].edits == [
        "Fetching Telegraph article…",
        "Downloading 2 image(s) from 'Telegraph Title'…",
        "Done!",
    ]
    assert uploads == [
        (userbot, ["Telegraph Title\nhttps://telegra.ph/example", ""], [first, second], "image-chat", None, None)
    ]
    assert first.exists() is False
    assert second.exists() is False


def test_telegraph_images_cleanup_on_upload_failure(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    event = fake_event(
        sender_id=42,
        text="see https://telegra.ph/example",
        message=make_message(id=502, text="see https://telegra.ph/example", message="see https://telegra.ph/example"),
    )
    image_path = tmp_path / "telegraph-fail.jpg"
    image_path.write_bytes(b"1")

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache=None)
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_IMAGE_CHANNEL", "@images")

    async def fake_resolve_chat(_client, _ref):
        return "image-chat"

    async def fake_fetch(_url):
        return "Telegraph Title", "body", ["https://telegra.ph/file/a.jpg"]

    async def fake_download(_urls):
        return [image_path]

    async def fake_upload(*_args, **_kwargs):
        raise RuntimeError("upload exploded")

    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "fetch_telegraph_page", fake_fetch)
    monkeypatch.setattr(bot_module, "download_telegraph_images", fake_download)
    monkeypatch.setattr(bot_module, "upload_downloaded_album", fake_upload)

    run(handle(event))

    assert event.replies[0].edits == [
        "Fetching Telegraph article…",
        "Downloading 1 image(s) from 'Telegraph Title'…",
        "Error: upload exploded",
    ]
    assert image_path.exists() is False


def test_archive_uses_shared_local_upload_and_cleans_temp_paths_on_success(
    tmp_path, fake_client, fake_event, make_message, monkeypatch
):
    bot_client = fake_client
    userbot = fake_client.__class__()
    archive_message = make_document_message(make_message, message_id=601, filename="bundle.zip")
    event = fake_event(sender_id=7, message=archive_message)
    temp_dir = tmp_path / "temp"
    extracted_image = temp_dir / "extract_601" / "image.jpg"
    extracted_video = temp_dir / "extract_601" / "video.mp4"
    uploads = []
    downloaded_archive = {}

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache=None)
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_IMAGE_CHANNEL", "@images")
    monkeypatch.setattr(bot_module.config, "DEFAULT_VIDEO_CHANNEL", "@videos")
    monkeypatch.setattr(bot_module.config, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(bot_module, "is_archive", lambda _message: True)

    async def fake_download_media(_message, *, file):
        downloaded_archive["path"] = Path(file)
        Path(file).write_bytes(b"archive-bytes")
        return file

    async def fake_extract(_archive_path, dest):
        extracted_image.parent.mkdir(parents=True, exist_ok=True)
        extracted_image.write_bytes(b"img")
        extracted_video.write_bytes(b"vid")
        return dest

    def fake_classify(_dest):
        return [extracted_image], [extracted_video]

    async def fake_resolve_chat(_client, ref):
        return {"@images": "image-chat", "@videos": "video-chat"}[ref]

    async def fake_upload(userbot_client, messages, media_files, target_chat, source_url=None, force_document=None, **_kwargs):
        uploads.append((userbot_client, [m.message for m in messages], [mf.path for mf in media_files], target_chat, source_url, force_document))
        return ["uploaded"]

    bot_client.download_media = fake_download_media
    monkeypatch.setattr(bot_module, "extract_archive", fake_extract)
    monkeypatch.setattr(bot_module, "classify_media", fake_classify)
    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "upload_downloaded_album", fake_upload)

    run(handle(event))

    assert event.replies[0].edits == [
        "Downloading archive…",
        "Extracting archive…",
        "Uploading 1 image(s)…",
        "Uploading 1 video(s)…",
        "Done!",
    ]
    assert uploads == [
        (userbot, [""], [extracted_image], "image-chat", None, None),
        (userbot, [""], [extracted_video], "video-chat", None, False),
    ]
    assert downloaded_archive["path"].exists() is False
    assert (temp_dir / "extract_601").exists() is False


def test_archive_cleans_temp_paths_on_upload_failure(tmp_path, fake_client, fake_event, make_message, monkeypatch):
    bot_client = fake_client
    userbot = fake_client.__class__()
    archive_message = make_document_message(make_message, message_id=602, filename="bundle.zip")
    event = fake_event(sender_id=7, message=archive_message)
    temp_dir = tmp_path / "temp"
    extracted_image = temp_dir / "extract_602" / "image.jpg"
    downloaded_archive = {}

    bot_module.register_handlers(bot_client, userbot, db=None, file_cache=None)
    handle = handler(bot_client, "handle_forwarded_or_target")

    monkeypatch.setattr(bot_module, "is_admin", lambda _sender_id: True)
    monkeypatch.setattr(bot_module.config, "DEFAULT_IMAGE_CHANNEL", "@images")
    monkeypatch.setattr(bot_module.config, "DEFAULT_VIDEO_CHANNEL", None)
    monkeypatch.setattr(bot_module.config, "TEMP_DIR", temp_dir)
    monkeypatch.setattr(bot_module, "is_archive", lambda _message: True)

    async def fake_download_media(_message, *, file):
        downloaded_archive["path"] = Path(file)
        Path(file).write_bytes(b"archive-bytes")
        return file

    async def fake_extract(_archive_path, dest):
        extracted_image.parent.mkdir(parents=True, exist_ok=True)
        extracted_image.write_bytes(b"img")
        return dest

    def fake_classify(_dest):
        return [extracted_image], []

    async def fake_resolve_chat(_client, _ref):
        return "image-chat"

    async def fake_upload(*_args, **_kwargs):
        raise RuntimeError("archive upload exploded")

    bot_client.download_media = fake_download_media
    monkeypatch.setattr(bot_module, "extract_archive", fake_extract)
    monkeypatch.setattr(bot_module, "classify_media", fake_classify)
    monkeypatch.setattr(bot_module, "resolve_chat", fake_resolve_chat)
    monkeypatch.setattr(bot_module, "upload_downloaded_album", fake_upload)

    run(handle(event))

    assert event.replies[0].edits == [
        "Downloading archive…",
        "Extracting archive…",
        "Uploading 1 image(s)…",
        "Error: archive upload exploded",
    ]
    assert downloaded_archive["path"].exists() is False
    assert (temp_dir / "extract_602").exists() is False
