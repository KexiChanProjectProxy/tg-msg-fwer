import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class FileCache:
    """LRU disk cache for downloaded media files.

    Files are stored flat in cache_dir as {file_unique_id}.
    Access time (atime) is used as the LRU key — no separate metadata DB.
    Eviction brings the cache down to 90% of max_size_bytes to avoid thrashing.
    All put/evict operations are protected by an asyncio.Lock.
    """

    def __init__(self, cache_dir: str, max_size_bytes: int):
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max = max_size_bytes
        self._lock = asyncio.Lock()

    def get(self, file_unique_id: str) -> Optional[Path]:
        """Return cached path if it exists, touching atime. Returns None on miss."""
        path = self._dir / file_unique_id
        if not path.exists():
            return None
        # Update access time to mark as recently used
        os.utime(path, None)
        logger.debug(f"Cache hit: {file_unique_id}")
        return path

    async def put(self, file_unique_id: str, data: bytes) -> Path:
        """Write data to cache, then evict if over the size limit."""
        path = self._dir / file_unique_id
        async with self._lock:
            path.write_bytes(data)
            logger.debug(f"Cache put: {file_unique_id} ({len(data)} bytes)")
            if self._current_size() > self._max:
                self._evict()
        return path

    async def put_file(self, file_unique_id: str, src: Path) -> Path:
        """Link or copy *src* into the cache without loading it into RAM."""
        path = self._dir / file_unique_id
        async with self._lock:
            try:
                os.link(src, path)
            except OSError:
                shutil.copy2(src, path)
            logger.debug(f"Cache put_file: {file_unique_id} ({path.stat().st_size} bytes)")
            if self._current_size() > self._max:
                self._evict()
        return path

    def _evict(self):
        """Delete LRU files until cache is at or below 90% of max size."""
        target = int(self._max * 0.9)
        files = sorted(self._dir.iterdir(), key=lambda p: p.stat().st_atime)
        for path in files:
            if self._current_size() <= target:
                break
            try:
                size = path.stat().st_size
                path.unlink()
                logger.debug(f"Cache evict: {path.name} ({size} bytes)")
            except OSError as e:
                logger.warning(f"Failed to evict {path.name}: {e}")

    def _current_size(self) -> int:
        """Return the total size of all files in the cache directory."""
        return sum(p.stat().st_size for p in self._dir.iterdir() if p.is_file())
