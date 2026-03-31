import asyncio
from pathlib import Path
from types import SimpleNamespace

import transfer


def _run(coro):
    return asyncio.run(coro)


class _FakeSenderPoolContext:
    def __init__(self, *_args, **kwargs):
        self._pool_size = kwargs.get("pool_size", 1)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def send(self, _request):
        return True


class _RecordingQueue(asyncio.Queue):
    created_maxsizes = []

    def __init__(self, maxsize=0):
        type(self).created_maxsizes.append(maxsize)
        super().__init__(maxsize=maxsize)


class _RecordingBudget:
    created_totals = []
    instances = []

    def __init__(self, total):
        self.total = total
        self.acquired = 0
        self.released = 0
        type(self).created_totals.append(total)
        type(self).instances.append(self)

    async def acquire(self, nbytes):
        self.acquired += nbytes

    async def release(self, nbytes):
        self.released += nbytes


class _IdleUserbot:
    async def get_messages(self, _chat, limit=None, **_kwargs):
        if limit == 1:
            return SimpleNamespace(total=0)
        return []

    async def iter_messages(self, *_args, **_kwargs):
        if False:
            yield None


def _async_return(value):
    async def _inner(*_args, **_kwargs):
        return value

    return _inner


async def _noop(*_args, **_kwargs):
    return None


async def _empty_ids(*_args, **_kwargs):
    return set()


def _patch_bulk_dependencies(monkeypatch):
    monkeypatch.setattr(transfer, "SenderPool", _FakeSenderPoolContext)
    monkeypatch.setattr(transfer.db_module, "update_job", _noop)
    monkeypatch.setattr(
        transfer.db_module,
        "get_job",
        _async_return(SimpleNamespace(source_chat="source", target_chat="target")),
    )
    monkeypatch.setattr(
        transfer.db_module, "get_transferred_ids_for_chat_pair", _empty_ids
    )
    monkeypatch.setattr(transfer.db_module, "record_transfers_batch", _noop)
    monkeypatch.setattr(transfer, "rate_limit_sleep", _noop)
    monkeypatch.setattr(transfer, "build_message_url", lambda *_args, **_kwargs: "source-url")


def test_transfer_bulk_queue_is_bounded_by_concurrent_transfers(monkeypatch):
    _RecordingQueue.created_maxsizes.clear()
    _patch_bulk_dependencies(monkeypatch)
    monkeypatch.setattr(transfer.asyncio, "Queue", _RecordingQueue)
    monkeypatch.setattr(transfer, "ByteBudget", _RecordingBudget)

    cfg = SimpleNamespace(
        CONCURRENT_TRANSFERS=7,
        MAX_CACHE_SIZE=1024,
        MAX_INFLIGHT_BYTES=256,
        SENDER_POOL_SIZE=1,
        TRANSFER_DELAY=0,
        STUCK_TIMEOUT=1,
    )

    _run(
        transfer.transfer_bulk(
            _IdleUserbot(), "source", "target", 1, object(), cfg, asyncio.Event()
        )
    )

    assert _RecordingQueue.created_maxsizes == [cfg.CONCURRENT_TRANSFERS]


def test_transfer_bulk_uses_dedicated_inflight_budget_not_cache_budget(monkeypatch):
    _RecordingBudget.created_totals.clear()
    _RecordingBudget.instances.clear()
    _patch_bulk_dependencies(monkeypatch)
    monkeypatch.setattr(transfer.asyncio, "Queue", _RecordingQueue)
    monkeypatch.setattr(transfer, "ByteBudget", _RecordingBudget)

    cfg = SimpleNamespace(
        CONCURRENT_TRANSFERS=3,
        MAX_CACHE_SIZE=16 * 1024 * 1024,
        MAX_INFLIGHT_BYTES=512 * 1024,
        SENDER_POOL_SIZE=1,
        TRANSFER_DELAY=0,
        STUCK_TIMEOUT=1,
    )

    _run(
        transfer.transfer_bulk(
            _IdleUserbot(), "source", "target", 1, object(), cfg, asyncio.Event()
        )
    )

    assert _RecordingBudget.created_totals == [cfg.MAX_INFLIGHT_BYTES]


class _ObservedSenderPool:
    def __init__(self, capacity):
        self.capacity = capacity
        self.current = 0
        self.max_inflight = 0
        self.entered = 0
        self.release_after = max(1, capacity)
        self.gate = asyncio.Event()

    async def send(self, _request):
        self.current += 1
        self.entered += 1
        self.max_inflight = max(self.max_inflight, self.current)
        if self.entered >= self.release_after:
            self.gate.set()
        await self.gate.wait()
        await asyncio.sleep(0)
        self.current -= 1
        return True


def test_upload_file_parallel_caps_sender_pool_inflight_requests(tmp_path):
    file_path = tmp_path / "large.bin"
    file_path.write_bytes(b"x" * (11 * 1024 * 1024))
    sender_pool = _ObservedSenderPool(capacity=2)

    _run(
        transfer._upload_file_parallel(
            userbot=object(),
            file_path=file_path,
            n_workers=4,
            sender_pool=sender_pool,
        )
    )

    assert sender_pool.max_inflight <= sender_pool.capacity


def test_upload_prepared_caps_parallel_workers_to_sender_pool_capacity(
    monkeypatch, tmp_path
):
    file_path = tmp_path / "prepared.bin"
    file_path.write_bytes(b"payload")
    requested_workers = []

    async def fake_upload_file_parallel(
        _userbot, path: Path, n_workers: int, sender_pool=None, on_progress=None
    ):
        requested_workers.append(n_workers)
        return path

    async def fake_send_file(*_args, **_kwargs):
        return SimpleNamespace(id=99)

    monkeypatch.setattr(transfer, "get_media_type", lambda _message: "document")
    monkeypatch.setattr(transfer, "_upload_file_parallel", fake_upload_file_parallel)
    monkeypatch.setattr(transfer, "_expand_telegraph", _noop)
    monkeypatch.setattr(transfer.config, "UPLOAD_WORKERS", 6)

    userbot = SimpleNamespace(send_file=fake_send_file)
    message = SimpleNamespace(
        id=5,
        action=None,
        media=SimpleNamespace(document=SimpleNamespace(attributes=[])),
        message="hello",
        entities=[],
    )
    media_file = transfer._MediaFile(file_path)
    sender_pool = SimpleNamespace(_pool_size=2)

    _run(
        transfer._upload_prepared(
            userbot,
            message,
            media_file,
            "target",
            "https://t.me/source/5",
            sender_pool=sender_pool,
        )
    )

    assert requested_workers == [sender_pool._pool_size]


def test_upload_prepared_sequential_send_file_uses_progress_callback(
    monkeypatch, tmp_path
):
    send_kwargs = []

    async def fake_send_file(*_args, **kwargs):
        send_kwargs.append(kwargs)
        return SimpleNamespace(id=77)

    async def fake_expand(*_args, **_kwargs):
        return None

    async def fake_faststart(path):
        return path

    async def unexpected_preupload(*_args, **_kwargs):
        raise AssertionError("sequential path should not pre-upload")

    monkeypatch.setattr(transfer.config, "UPLOAD_WORKERS", 1)
    monkeypatch.setattr(transfer, "_expand_telegraph", fake_expand)
    monkeypatch.setattr(transfer, "ensure_faststart", fake_faststart)
    monkeypatch.setattr(transfer, "needs_video_conversion", lambda _msg: False)
    monkeypatch.setattr(transfer, "_upload_file_parallel", unexpected_preupload)

    progress_cb = lambda _current, _total: None
    userbot = SimpleNamespace(send_file=fake_send_file)

    for i, media_type in enumerate(("document", "voice", "video_note", "animation", "video"), start=1):
        path = tmp_path / f"{media_type}.bin"
        path.write_bytes(b"payload")
        message = SimpleNamespace(
            id=i,
            action=None,
            media=SimpleNamespace(document=SimpleNamespace(attributes=[])),
            message="caption",
            entities=[],
        )
        _run(
            transfer._upload_prepared(
                userbot,
                message,
                transfer._MediaFile(path),
                target_chat="target",
                source_url="https://t.me/src/1",
                media_type=media_type,
                progress_cb=progress_cb,
            )
        )

    assert len(send_kwargs) == 5
    for kwargs in send_kwargs:
        assert kwargs["progress_callback"] is progress_cb


def test_transfer_bulk_cancellation_cleans_queued_files_and_releases_budget(
    monkeypatch, tmp_path
):
    _RecordingBudget.instances.clear()

    class FakeMediaFile:
        def __init__(self, path):
            self.path = path
            self.cleaned = False

        def cleanup(self):
            self.cleaned = True

    class FakeUserbot:
        async def get_messages(self, _chat, limit=None, **_kwargs):
            if limit == 1:
                return SimpleNamespace(total=2)
            return []

        async def iter_messages(self, *_args, **_kwargs):
            yield message_one
            yield message_two

    async def fake_download_to_file(_userbot, message, cache=None, on_progress=None):
        return media_files[message.id]

    async def blocked_upload_prepared(*_args, **_kwargs):
        await asyncio.Event().wait()

    original_sleep = asyncio.sleep
    cancel_event = asyncio.Event()

    async def fast_sleep(_delay):
        if not cancel_event.is_set():
            cancel_event.set()
        await original_sleep(0)

    message_one = SimpleNamespace(
        id=1,
        action=None,
        grouped_id=None,
        media=SimpleNamespace(document=SimpleNamespace(size=5)),
        message="one",
        entities=[],
    )
    message_two = SimpleNamespace(
        id=2,
        action=None,
        grouped_id=None,
        media=SimpleNamespace(document=SimpleNamespace(size=7)),
        message="two",
        entities=[],
    )

    path_one = tmp_path / "one.bin"
    path_one.write_bytes(b"1" * 5)
    path_two = tmp_path / "two.bin"
    path_two.write_bytes(b"2" * 7)
    media_files = {
        1: FakeMediaFile(path_one),
        2: FakeMediaFile(path_two),
    }

    _patch_bulk_dependencies(monkeypatch)
    monkeypatch.setattr(transfer, "ByteBudget", _RecordingBudget)
    monkeypatch.setattr(transfer, "_download_to_file", fake_download_to_file)
    monkeypatch.setattr(transfer, "_upload_prepared", blocked_upload_prepared)
    monkeypatch.setattr(transfer.asyncio, "sleep", fast_sleep)

    cfg = SimpleNamespace(
        CONCURRENT_TRANSFERS=2,
        MAX_CACHE_SIZE=1024,
        MAX_INFLIGHT_BYTES=1024,
        SENDER_POOL_SIZE=1,
        TRANSFER_DELAY=0,
        STUCK_TIMEOUT=1,
    )

    _run(
        transfer.transfer_bulk(
            FakeUserbot(), "source", "target", 1, object(), cfg, cancel_event
        )
    )

    budget = _RecordingBudget.instances[-1]
    assert [media_files[1].cleaned, media_files[2].cleaned] == [True, True]
    assert budget.released == 12
