import asyncio
import logging

from telethon import TelegramClient
from telethon.network import MTProtoSender

logger = logging.getLogger(__name__)


class ByteBudget:
    """Async byte-counting semaphore for disk-space backpressure.

    acquire(n) blocks while (used > 0 and used + n > total), so files
    larger than the total budget are still allowed through one at a time
    when the disk is otherwise empty.
    """

    def __init__(self, total: int):
        self._total = total
        self._used = 0
        self._cond = asyncio.Condition()

    async def acquire(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        async with self._cond:
            while self._used > 0 and self._used + nbytes > self._total:
                await self._cond.wait()
            self._used += nbytes

    async def release(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        async with self._cond:
            self._used = max(0, self._used - nbytes)
            self._cond.notify_all()


class SenderPool:
    """N independent MTProto TCP connections to the same DC.

    Reuses the client's auth_key (no DH handshake) and dispatches requests
    round-robin so all connections stay saturated simultaneously.

    Usage::

        async with SenderPool(client, pool_size=4) as pool:
            result = await pool.send(SaveBigFilePartRequest(...))
    """

    def __init__(self, client: TelegramClient, pool_size: int):
        self._client = client
        self._pool_size = max(1, pool_size)
        self._senders: list = []
        self._idx = 0
        self._rr_lock = asyncio.Lock()

    async def _new_sender(self) -> MTProtoSender:
        client = self._client
        sender = MTProtoSender(
            client.session.auth_key,
            loggers=client._log,
            retries=getattr(client, "_connection_retries", 5),
            delay=getattr(client, "_retry_delay", 1),
            auto_reconnect=getattr(client, "_auto_reconnect", True),
            connect_timeout=getattr(client, "_timeout", 10),
        )
        connection = client._connection(
            client.session.server_address,
            client.session.port,
            client.session.dc_id,
            loggers=client._log,
            proxy=getattr(client, "_proxy", None),
            local_addr=getattr(client, "_local_addr", None),
        )
        await sender.connect(connection)
        logger.debug(
            "SenderPool: connected to DC%d (%s:%d)",
            client.session.dc_id,
            client.session.server_address,
            client.session.port,
        )
        return sender

    async def __aenter__(self) -> "SenderPool":
        for i in range(self._pool_size):
            try:
                self._senders.append(await self._new_sender())
            except Exception as exc:
                logger.warning("SenderPool: sender %d failed to connect: %s", i, exc)
        if not self._senders:
            raise RuntimeError("SenderPool: could not create any connections")
        logger.info(
            "SenderPool: %d/%d connections ready", len(self._senders), self._pool_size
        )
        return self

    async def __aexit__(self, *_) -> None:
        for s in self._senders:
            try:
                await s.disconnect()
            except Exception:
                pass
        self._senders.clear()

    async def send(self, request):
        """Round-robin dispatch; reconnects a dead sender once on failure."""
        async with self._rr_lock:
            n = len(self._senders)
            idx = self._idx % n
            self._idx += 1

        sender = self._senders[idx]
        try:
            return await sender.send(request)
        except Exception as exc:
            logger.warning("SenderPool: sender %d failed (%s), reconnecting", idx, exc)
            try:
                new = await self._new_sender()
                self._senders[idx] = new
                return await new.send(request)
            except Exception as exc2:
                logger.error(
                    "SenderPool: reconnect of sender %d failed: %s", idx, exc2
                )
                raise
