"""Configuration settings for ram-io-spooler."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # Staging paths in high-speed RAM (tmpfs)
    spool_dir: str = os.getenv("SPOOL_DIR", "/dev/shm/io-spool")
    socket_path: str = os.getenv("SPOOL_SOCKET", "/dev/shm/io-spool/spooler.sock")
    staging_dir: str = os.getenv("SPOOL_STAGING", "/dev/shm/io-spool/staging")

    # Flushing triggers & thresholds
    flush_interval_sec: float = float(os.getenv("SPOOL_FLUSH_INTERVAL", "3.0"))
    max_batch_items: int = int(os.getenv("SPOOL_MAX_BATCH_ITEMS", "1000"))
    max_batch_bytes: int = int(os.getenv("SPOOL_MAX_BATCH_BYTES", str(32 * 1024 * 1024)))  # 32 MB

    # Max RAM allowed in tmpfs staging before backpressure/forced flush
    max_staging_bytes: int = int(os.getenv("SPOOL_MAX_STAGING_BYTES", str(512 * 1024 * 1024)))  # 512 MB

    # Threshold for storing payload in-memory vs tmpfs file
    in_memory_threshold_bytes: int = int(os.getenv("SPOOL_IN_MEMORY_THRESHOLD", str(64 * 1024)))  # 64 KB

    # Retention of completed task metadata for query inspection
    history_limit: int = int(os.getenv("SPOOL_HISTORY_LIMIT", "500"))

    # I/O chunk size during physical disk write
    write_chunk_size: int = int(os.getenv("SPOOL_CHUNK_SIZE", str(128 * 1024)))  # 128 KB

    # HTTP API Configuration
    http_enabled: bool = os.getenv("SPOOL_HTTP_ENABLED", "1").lower() in ("1", "true", "yes")
    http_host: str = os.getenv("SPOOL_HTTP_HOST", "127.0.0.1")
    http_port: int = int(os.getenv("SPOOL_HTTP_PORT", "8080"))

    # Fallback TCP port for Windows or network socket mode
    fallback_tcp_port: int = int(os.getenv("SPOOL_TCP_PORT", "49152"))


SpoolerConfig = Config

