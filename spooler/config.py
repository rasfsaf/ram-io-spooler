"""Configuration for RAM Write-Behind Spooler."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class SpoolerConfig:
    """Spooler engine and server configuration."""
    
    # Storage and paths
    shm_dir: str = "/dev/shm/io-spool" if os.path.exists("/dev/shm") else os.path.join(os.environ.get("TEMP", "/tmp"), "io-spool")
    spool_dir: str = ""
    staging_dir: str = ""
    socket_path: str = "/run/ram-spooler.sock" if os.path.exists("/run") else os.path.join(shm_dir, "ram-spooler.sock")
    ipc_socket_path: str = ""
    ipc_fallback_port: int = 9180
    http_enabled: bool = True
    http_host: str = "127.0.0.1"
    http_port: int = 8990
    
    # Limits & Timing
    in_memory_threshold_bytes: int = 65536
    max_queue_items: int = 50000
    max_ram_bytes: int = 512 * 1024 * 1024       # 512 MB transit RAM buffer
    max_queue_memory_bytes: int = 512 * 1024 * 1024
    max_file_size: int = 128 * 1024 * 1024       # 128 MB max single payload
    flush_interval_sec: float = 3.0              # Background flush timer
    flush_interval_seconds: float = 3.0
    max_batch_size: int = 500                    # Coalescing batch chunk
    max_batch_items: int = 500
    max_coalesce_tasks: int = 500
    
    # Read Cache (LRU in-memory read accelerator)
    read_cache_enabled: bool = True
    read_cache_max_bytes: int = 1024 * 1024 * 1024  # 1 GB RAM Read Cache
    read_cache_max_file_size: int = 64 * 1024 * 1024 # 64 MB max single file cache
    read_cache_ttl_seconds: float = 300.0        # 5 minutes TTL
    read_cache_ttl_sec: float = 300.0            # Compatibility alias
    
    # Advanced I/O
    force_fsync: bool = True                     # Call fsync() on flushed files
    fsync_enabled: bool = True                   # Alias for force_fsync
    fadvise_dontneed_after_write: bool = True     # Evict written dirty pages from page cache
    fadvise_dontneed: bool = True                 # Alias for fadvise_dontneed_after_write
    coalesce_writes: bool = True                 # Merge repeated writes to same path
    elevator_sort: bool = True                   # Sort paths to optimize HDD head seek

    def __post_init__(self):
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


# Alias for backward compatibility
Config = SpoolerConfig
default_config = SpoolerConfig()
config = default_config
