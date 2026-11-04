import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Optional


def parse_size_bytes(size: Union[int, float, str]) -> int:
    """
    Parse a human-readable size string (e.g. '1GB', '512MB', '2G', '1.5GB', '1024K')
    or integer into exact number of bytes.
    """
    if isinstance(size, (int, float)):
        return int(size)
    if not isinstance(size, str):
        raise TypeError(f"Invalid size type: {type(size)}")
    
    s = size.strip().upper()
    if not s:
        return 0
    if s.isdigit():
        return int(s)
    
    units = {
        "B": 1,
        "K": 1024,
        "KB": 1024,
        "KIB": 1024,
        "M": 1024 * 1024,
        "MB": 1024 * 1024,
        "MIB": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "GIB": 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
        "TIB": 1024 * 1024 * 1024 * 1024,
    }
    
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([A-Z]+)$", s)
    if m:
        val = float(m.group(1))
        unit = m.group(2)
        if unit in units:
            return int(val * units[unit])
    
    raise ValueError(f"Invalid size format: '{size}'. Expected formats: '1GB', '512MB', '1.5G', '1024K', or raw integer bytes.")


def _get_env_size(var_names: list[str], default_bytes: int) -> int:
    """Retrieve size in bytes from the first defined environment variable, or fallback to default."""
    for var in var_names:
        val = os.environ.get(var)
        if val is not None and val.strip():
            try:
                return parse_size_bytes(val)
            except ValueError:
                pass
    return default_bytes


@dataclass
class SpoolerConfig:
    # Storage & Buffering
    shm_dir: Path = field(default_factory=lambda: Path(os.environ.get("SPOOLER_SHM_DIR", "/dev/shm/ram_spooler")))
    spool_dir: Optional[Path] = None
    staging_dir: Optional[Path] = None
    socket_path: Path = field(default_factory=lambda: Path(os.environ.get("SPOOLER_SOCKET_PATH", "/run/ram-spooler.sock")))
    ipc_socket_path: Optional[Path] = None
    http_host: str = field(default_factory=lambda: os.environ.get("SPOOLER_HTTP_HOST", "127.0.0.1"))
    http_port: int = field(default_factory=lambda: int(os.environ.get("SPOOLER_HTTP_PORT", "9095")))
    ipc_fallback_port: int = 9095
    http_enabled: bool = True
    
    # Flushing & Batching
    flush_interval_sec: float = field(default_factory=lambda: float(os.environ.get("SPOOLER_FLUSH_INTERVAL", "3.0")))
    flush_interval_seconds: float = 3.0
    max_ram_bytes: Union[int, str] = field(
        default_factory=lambda: _get_env_size(
            ["SPOOLER_WRITE_BUFFER_SIZE", "SPOOLER_MAX_RAM_BYTES", "SPOOLER_WRITE_BUFFER"],
            512 * 1024 * 1024  # 512 MB default write buffer
        )
    )
    write_buffer_size: Optional[Union[int, str]] = None
    max_queue_memory_bytes: Union[int, str] = 512 * 1024 * 1024
    max_batch_size: int = 500                    # Coalescing batch chunk
    max_batch_items: int = 500
    max_coalesce_tasks: int = 500
    
    # Read Cache (LRU in-memory read accelerator)
    read_cache_enabled: bool = field(
        default_factory=lambda: os.environ.get("SPOOLER_READ_CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
    )
    read_cache_max_bytes: Union[int, str] = field(
        default_factory=lambda: _get_env_size(
            ["SPOOLER_READ_CACHE_SIZE", "SPOOLER_READ_CACHE_BYTES", "SPOOLER_CACHE_SIZE", "SPOOLER_BUFFER_SIZE"],
            1024 * 1024 * 1024  # 1 GB default RAM Read Cache
        )
    )
    read_cache_size: Optional[Union[int, str]] = None
    read_cache_max_file_size: Union[int, str] = field(
        default_factory=lambda: _get_env_size(
            ["SPOOLER_READ_CACHE_MAX_FILE_SIZE"],
            64 * 1024 * 1024  # 64 MB max single file cache
        )
    )
    read_cache_ttl_seconds: float = field(
        default_factory=lambda: float(os.environ.get("SPOOLER_READ_CACHE_TTL", "300.0"))
    )
    read_cache_ttl_sec: float = 300.0
    
    # Advanced I/O
    force_fsync: bool = True                     # Call fsync() on flushed files
    fsync_enabled: bool = True                   # Alias for force_fsync
    fadvise_dontneed_after_write: bool = True     # Evict written dirty pages from page cache
    fadvise_dontneed: bool = True                 # Alias for fadvise_dontneed_after_write
    coalesce_writes: bool = True                 # Merge repeated writes to same path
    elevator_sort: bool = True                   # Sort paths to optimize HDD head seek

    def __post_init__(self):
        # Override with explicit aliases if provided
        if self.write_buffer_size is not None:
            self.max_ram_bytes = self.write_buffer_size
        if self.read_cache_size is not None:
            self.read_cache_max_bytes = self.read_cache_size

        # Normalize size attributes to integer bytes
        self.max_ram_bytes = parse_size_bytes(self.max_ram_bytes)
        self.max_queue_memory_bytes = self.max_ram_bytes
        self.read_cache_max_bytes = parse_size_bytes(self.read_cache_max_bytes)
        self.read_cache_max_file_size = parse_size_bytes(self.read_cache_max_file_size)

        # Sync paths & aliases
        if self.ipc_socket_path and not self.socket_path:
            self.socket_path = self.ipc_socket_path
        elif self.socket_path and not self.ipc_socket_path:
            self.ipc_socket_path = self.socket_path
        if self.staging_dir and not self.spool_dir:
            self.spool_dir = self.staging_dir
        if self.spool_dir:
            self.shm_dir = self.spool_dir
        elif self.shm_dir:
            self.spool_dir = self.shm_dir
        if self.flush_interval_sec != 3.0 and self.flush_interval_seconds == 3.0:
            self.flush_interval_seconds = self.flush_interval_sec
        elif self.flush_interval_seconds != 3.0 and self.flush_interval_sec == 3.0:
            self.flush_interval_sec = self.flush_interval_seconds
        if not self.staging_dir:
            self.staging_dir = self.shm_dir
        if self.read_cache_ttl_sec != 300.0 and self.read_cache_ttl_seconds == 300.0:
            self.read_cache_ttl_seconds = self.read_cache_ttl_sec
        elif self.read_cache_ttl_seconds != 300.0 and self.read_cache_ttl_sec == 300.0:
            self.read_cache_ttl_sec = self.read_cache_ttl_seconds


# Alias for backward compatibility
Config = SpoolerConfig
default_config = SpoolerConfig()
config = default_config
