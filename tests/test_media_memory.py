import asyncio
import io
from pathlib import Path
from types import SimpleNamespace

import media
import pytest
import transfer


def _run(coro):
    return asyncio.run(coro)


class _NoFullReadBuffer(io.BytesIO):
    def read(self, size=-1):
        if size == -1:
            raise AssertionError("probe_extension should avoid full-buffer reads")
        return super().read(size)


def test_probe_extension_does_not_full_read_unknown_buffers(monkeypatch):
    monkeypatch.setattr(media, "guess_extension_from_bytes", lambda _buf: "bin")
    monkeypatch.setattr(media, "_probe_file_sync", lambda _path: "bin")

    buf = _NoFullReadBuffer(b"not-a-known-header" * 8192)

    assert _run(media.probe_extension(buf)) == "bin"
    assert buf.tell() == 0


class _FakeUserbot:
    def __init__(self, *, fail_send=False):
        self.fail_send = fail_send

    async def send_file(self, *_args, **_kwargs):
        if self.fail_send:
            raise RuntimeError("send failed")
        return SimpleNamespace(id=123)


def _video_message(msg_id=42):
    return SimpleNamespace(
        id=msg_id,
        action=None,
        media=SimpleNamespace(document=SimpleNamespace(attributes=[])),
        message="caption",
        entities=[],
    )


def _touch(path: Path):
    path.write_bytes(b"x")
    return path


def test_upload_prepared_conversion_replaced_path_is_cleaned(monkeypatch, tmp_path):
    old_path = _touch(tmp_path / "old.bin")
    new_path = _touch(tmp_path / "new.mp4")
    mf = transfer._MediaFile(old_path, owned=True)

    async def fake_convert(_path):
        return new_path

    async def fake_preupload(*_args, **_kwargs):
        return "handle"

    async def fake_expand(*_args, **_kwargs):
        return None

    monkeypatch.setattr(transfer, "get_media_type", lambda _msg: "video")
    monkeypatch.setattr(transfer, "needs_video_conversion", lambda _msg: True)
    monkeypatch.setattr(transfer, "convert_video", fake_convert)
    monkeypatch.setattr(transfer, "_upload_file_parallel", fake_preupload)
    monkeypatch.setattr(transfer, "_expand_telegraph", fake_expand)

    result = _run(
        transfer._upload_prepared(
            _FakeUserbot(),
            _video_message(),
            mf,
            target_chat="target",
            source_url="https://t.me/src/1",
            media_type="video",
            sender_pool=None,
        )
    )

    assert result.id == 123
    assert not old_path.exists()
    assert mf.path == new_path
    mf.cleanup()
    assert not new_path.exists()


def test_upload_prepared_faststart_replaced_path_cleanup_with_exception(monkeypatch, tmp_path):
    old_path = _touch(tmp_path / "old.mp4")
    new_path = _touch(tmp_path / "new.mp4")
    mf = transfer._MediaFile(old_path, owned=True)

    async def fake_faststart(_path):
        return new_path

    async def fake_preupload(*_args, **_kwargs):
        return "handle"

    async def fake_expand(*_args, **_kwargs):
        return None

    monkeypatch.setattr(transfer, "get_media_type", lambda _msg: "video")
    monkeypatch.setattr(transfer, "needs_video_conversion", lambda _msg: False)
    monkeypatch.setattr(transfer, "ensure_faststart", fake_faststart)
    monkeypatch.setattr(transfer, "_upload_file_parallel", fake_preupload)
    monkeypatch.setattr(transfer, "_expand_telegraph", fake_expand)

    with pytest.raises(RuntimeError, match="send failed"):
        _run(
            transfer._upload_prepared(
                _FakeUserbot(fail_send=True),
                _video_message(),
                mf,
                target_chat="target",
                source_url="https://t.me/src/1",
                media_type="video",
                sender_pool=None,
            )
        )

    assert not old_path.exists()
    assert mf.path == new_path
    mf.cleanup()
    assert not new_path.exists()
