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


def _find_ipv6_addr(client: TelegramClient) -> "tuple[str, int] | None":
    """Return (ip, port) for the current DC's IPv6 endpoint, or None."""
    dc_id = client.session.dc_id
    cfg = getattr(client, "_config", None)
    if cfg is None:
        return None
    for opt in getattr(cfg, "dc_options", []):
        if (
            opt.id == dc_id
            and getattr(opt, "ipv6", False)
            and not getattr(opt, "cdn", False)
            and not getattr(opt, "media_only", False)
        ):
            return opt.ip_address, opt.port
    return None


class SenderPool:
    """N independent MTProto TCP connections to the same DC.

    Reuses the client's auth_key (no DH handshake) and dispatches requests
    round-robin so all connections stay saturated simultaneously.

    When the DC advertises an IPv6 address (via client._config.dc_options),
    pool slots are interleaved between IPv4 and IPv6:
        slot 0, 2, 4, … → IPv4
        slot 1, 3, 5, … → IPv6
    This spreads upload traffic across both network paths for extra throughput.
    If IPv6 is unavailable all slots use IPv4.

    Usage::

        async with SenderPool(client, pool_size=4) as pool:
            result = await pool.send(SaveBigFilePartRequest(...))
    """

    def __init__(self, client: TelegramClient, pool_size: int):
        self._client = client
        self._pool_size = max(1, pool_size)
        self._senders: list = []
        # Parallel list: (ip, port) used by each sender slot for reconnects.
        self._endpoints: list = []
        self._idx = 0
        self._rr_lock = asyncio.Lock()

    async def _new_sender(self, ip: str, port: int) -> MTProtoSender:
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
            ip,
            port,
            client.session.dc_id,
            loggers=client._log,
            proxy=getattr(client, "_proxy", None),
            local_addr=getattr(client, "_local_addr", None),
        )
        await sender.connect(connection)
        logger.debug(
            "SenderPool: connected to DC%d (%s:%d)",
            client.session.dc_id,
            ip,
            port,
        )
        return sender

    async def __aenter__(self) -> "SenderPool":
        client = self._client
        ipv4 = (client.session.server_address, client.session.port)
        ipv6 = _find_ipv6_addr(client)

        if ipv6:
            logger.info(
                "SenderPool: IPv6 available for DC%d at %s:%d — alternating IPv4/IPv6",
                client.session.dc_id,
                ipv6[0],
                ipv6[1],
            )
        else:
            logger.debug(
                "SenderPool: no IPv6 address found for DC%d, using IPv4 only",
                client.session.dc_id,
            )

        for i in range(self._pool_size):
            # Even slots → IPv4, odd slots → IPv6 (when available)
            ip, port = (ipv6 if ipv6 and i % 2 == 1 else ipv4)
            try:
                sender = await self._new_sender(ip, port)
                self._senders.append(sender)
                self._endpoints.append((ip, port))
            except Exception as exc:
                logger.warning(
                    "SenderPool: sender %d (%s:%d) failed to connect: %s",
                    i, ip, port, exc,
                )

        if not self._senders:
            raise RuntimeError("SenderPool: could not create any connections")
        v4 = sum(1 for ip, _ in self._endpoints if ":" not in ip)
        v6 = len(self._endpoints) - v4
        logger.info(
            "SenderPool: %d/%d connections ready (%d IPv4, %d IPv6)",
            len(self._senders), self._pool_size, v4, v6,
        )
        return self

    async def __aexit__(self, *_) -> None:
        for s in self._senders:
            try:
                await s.disconnect()
            except Exception:
                pass
        self._senders.clear()
        self._endpoints.clear()

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
            ip, port = self._endpoints[idx]
            logger.warning(
                "SenderPool: sender %d (%s:%d) failed (%s), reconnecting",
                idx, ip, port, exc,
            )
            try:
                new = await self._new_sender(ip, port)
                self._senders[idx] = new
                return await new.send(request)
            except Exception as exc2:
                logger.error(
                    "SenderPool: reconnect of sender %d (%s:%d) failed: %s",
                    idx, ip, port, exc2,
                )
                raise
