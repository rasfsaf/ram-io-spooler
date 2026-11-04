import os
import threading
import time
from collections import OrderedDict
from typing import Dict, Optional, Tuple, Any

class ReadCacheEntry:
    __slots__ = ("path", "data", "size", "mtime", "cached_at", "hits")

    def __init__(self, path: str, data: bytes, mtime: float):
        self.path: str = path
        self.data: bytes = data
        self.size: int = len(data)
        self.mtime: float = mtime
        self.cached_at: float = time.time()
        self.hits: int = 0

class RAMReadCache:
    """
    High-speed in-memory LRU read cache for slow storage.
    Enforces strict max_size_bytes limit and handles invalidation.
    """

    def __init__(
        self,
        max_size_bytes: int = 1024 * 1024 * 1024,
        max_single_file_bytes: int = 128 * 1024 * 1024,
        max_bytes: Optional[int] = None,
        max_file_size: Optional[int] = None,
        ttl_seconds: float = 300.0,
    ):
        if max_bytes is not None:
            max_size_bytes = max_bytes
        if max_file_size is not None:
            max_single_file_bytes = max_file_size

        self.max_size_bytes: int = max_size_bytes
        self.max_single_file_bytes: int = max_single_file_bytes
        self.ttl_seconds: float = ttl_seconds
        self._current_size_bytes: int = 0
        self._entries: OrderedDict[str, ReadCacheEntry] = OrderedDict()
        self._lock: threading.RLock = threading.RLock()
        self._hits: int = 0
        self._misses: int = 0

    @property
    def current_size_bytes(self) -> int:
        with self._lock:
            return self._current_size_bytes

    def get(self, path: str) -> Optional[bytes]:
        """
        Retrieve data from cache if present and fresh.
        Returns None on miss, expired TTL, or external modification.
        """
        norm_path = os.path.normpath(os.path.abspath(path))
        with self._lock:
            entry = self._entries.get(norm_path)
            if entry is None:
                self._misses += 1
                return None

            # Check TTL
            if self.ttl_seconds > 0 and (time.time() - entry.cached_at > self.ttl_seconds):
                self._remove_entry(norm_path)
                self._misses += 1
                return None

            # Check if file changed on disk behind our back
            if os.path.exists(norm_path):
                try:
                    stat = os.stat(norm_path)
                    if stat.st_mtime > entry.mtime:
                        self._remove_entry(norm_path)
                        self._misses += 1
                        return None
                except OSError:
                    pass

            entry.hits += 1
            self._hits += 1
            # Move to end (most recently used)
            self._entries.move_to_end(norm_path)
            return entry.data

    def put(self, path: str, data: bytes) -> bool:
        """
        Insert or update data in the read cache.
        Evicts oldest entries if capacity is exceeded.
        """
        size = len(data)
        if size > self.max_single_file_bytes or size > self.max_size_bytes:
            return False

        norm_path = os.path.normpath(os.path.abspath(path))
        try:
            mtime = os.path.getmtime(norm_path) if os.path.exists(norm_path) else time.time()
        except OSError:
            mtime = time.time()

        with self._lock:
            if norm_path in self._entries:
                self._remove_entry(norm_path)

            # Evict until there is enough space
            while self._entries and (self._current_size_bytes + size > self.max_size_bytes):
                # Pop oldest (first item in OrderedDict)
                oldest_path, _ = self._entries.popitem(last=False)
                # Note: size was already accounted for, but we need to subtract it
                # To be precise, fetch entry before popping
                pass

            # Precise eviction
            while self._entries and (self._current_size_bytes + size > self.max_size_bytes):
                oldest_key = next(iter(self._entries))
                self._remove_entry(oldest_key)

            entry = ReadCacheEntry(norm_path, data, mtime)
            self._entries[norm_path] = entry
            self._current_size_bytes += size
            return True

    def invalidate(self, path: str) -> bool:
        """Explicitly evict a path from cache (called upon write/spool)."""
        norm_path = os.path.normpath(os.path.abspath(path))
        with self._lock:
            if norm_path in self._entries:
                self._remove_entry(norm_path)
                return True
            return False

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._entries.clear()
            self._current_size_bytes = 0

    def _remove_entry(self, norm_path: str) -> None:
        entry = self._entries.pop(norm_path, None)
        if entry is not None:
            self._current_size_bytes = max(0, self._current_size_bytes - entry.size)

    @property
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            total_reqs = self._hits + self._misses
            hit_ratio = (self._hits / total_reqs) if total_reqs > 0 else 0.0
            return {
                "entries_count": len(self._entries),
                "items_count": len(self._entries),
                "current_size_bytes": self._current_size_bytes,
                "current_size_mb": round(self._current_size_bytes / (1024 * 1024), 2),
                "max_size_bytes": self.max_size_bytes,
                "max_size_mb": round(self.max_size_bytes / (1024 * 1024), 2),
                "hits": self._hits,
                "misses": self._misses,
                "hit_ratio": round(hit_ratio, 4),
            }

    def get_stats(self) -> Dict[str, Any]:
        return self.stats
