import os
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest


os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "test-hash")
os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("ADMIN_IDS", "")

import bot as bot_module


@dataclass
class FakeMessage:
    id: int
    text: str = ""
    message: str | None = None
    media: object = None
    entities: list = field(default_factory=list)
    grouped_id: int | None = None
    action: object = None
    fwd_from: object = None
    has_media: bool = False

    def __post_init__(self):
        if self.message is None:
            self.message = self.text
        if self.has_media and self.media is None:
            self.media = object()


class FakeStatusMessage:
    def __init__(self, text: str):
        self.text = text
        self.edits = []

    async def edit(self, text: str):
        self.edits.append(text)
        self.text = text
        return self


class FakeEvent:
    def __init__(self, *, sender_id=1, text="", message=None):
        self.sender_id = sender_id
        self.text = text
        self.message = message or FakeMessage(id=1, text=text, message=text)
        self.media = self.message.media
        self.replies = []

    async def reply(self, text: str):
        status = FakeStatusMessage(text)
        self.replies.append(status)
        return status


class FakeTelethonClient:
    def __init__(self):
        self.sent_messages = []
        self.sent_files = []
        self.handlers = {}
        self.bot_messages = []

    def on(self, *_args, **_kwargs):
        def decorator(func):
            self.handlers[func.__name__] = func
            return func

        return decorator

    async def get_messages(self, _chat, ids):
        return FakeMessage(id=ids, text=f"message-{ids}", has_media=False)

    async def send_message(self, chat, text=None, **kwargs):
        rendered = text if text is not None else kwargs.get("message")
        self.sent_messages.append((chat, rendered, kwargs))
        status = FakeStatusMessage(rendered or "")
        self.bot_messages.append((chat, status))
        return status

    async def send_file(self, chat, **kwargs):
        self.sent_files.append((chat, kwargs))
        return kwargs.get("file")

    async def upload_file(self, file, **_kwargs):
        return Path(file)


@pytest.fixture(autouse=True)
def clear_pending_forwards():
    bot_module.pending_forwards.clear()
    yield
    bot_module.pending_forwards.clear()


@pytest.fixture
def fake_client():
    return FakeTelethonClient()


@pytest.fixture
def fake_source_message():
    return FakeMessage(id=1, text="hello", has_media=True)


@pytest.fixture
def make_message():
    def _make_message(**kwargs):
        return FakeMessage(**kwargs)

    return _make_message


@pytest.fixture
def fake_event():
    def _fake_event(**kwargs):
        return FakeEvent(**kwargs)

    return _fake_event


@pytest.fixture
def simple_namespace():
    return SimpleNamespace
