import asyncio
import logging
import functools
import random
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def build_message_url(chat, msg_id: int) -> str:
    """Build a t.me URL for a message. Works for both public and private chats."""
    if getattr(chat, "username", None):
        return f"https://t.me/{chat.username}/{msg_id}"
    # Private channel: bare channel ID (without -100 prefix)
    return f"https://t.me/c/{chat.id}/{msg_id}"


async def rate_limit_sleep(base_delay: float):
    """Sleep for base_delay ± 0.5s to avoid fixed-interval detection patterns."""
    jitter = random.uniform(-0.5, 0.5)
    await asyncio.sleep(max(0.0, base_delay + jitter))


def retry_on_flood(max_retries: int = 3):
    """Decorator that retries on FloodWaitError with exponential back-off."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except FloodWaitError as e:
                    if attempt == max_retries - 1:
                        raise
                    wait = e.seconds + 5
                    logger.warning(
                        f"FloodWait: sleeping {wait}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
        return wrapper
    return decorator
