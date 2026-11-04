"""
RAM Write-Behind Spooler - Core Server Daemon.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import socket
import sys
import threading
import time
from typing import Any, Dict, Optional, Tuple

from .config import SpoolerConfig, config as default_config
from .elevator import LBAPathElevator
from .flusher import Flusher
from .http_server import SpoolerHttpServer
from .read_cache import RAMReadCache
from .state import TaskManager, TaskState

logger = logging.getLogger("ram_spooler.server")


class SpoolerServer:
    """Core daemon managing memory staging, queueing, LRU read cache, and disk flushing."""

    def __init__(self, config: Optional[SpoolerConfig] = None):
        self.config = config or default_config
        self.task_manager = TaskManager(self.config)
        self.elevator = LBAPathElevator()
        self.flusher = Flusher(
            task_manager=self.task_manager,
            fsync_enabled=self.config.fsync_enabled,
            fadvise_dontneed=self.config.fadvise_dontneed,
        )
        self.read_cache = (
            RAMReadCache(
                max_bytes=self.config.read_cache_max_bytes,
                max_file_size=self.config.read_cache_max_file_size,
                ttl_seconds=self.config.read_cache_ttl_seconds,
            )
            if getattr(self.config, "read_cache_enabled", True)
            else None
        )

        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
        self._ipc_thread: Optional[threading.Thread] = None
        self._flush_trigger = threading.Event()
        self._server_sock: Optional[socket.socket] = None
        self.http_server = (
            SpoolerHttpServer(self.config, self)
            if getattr(self.config, "http_enabled", False)
            else None
        )
        self._stats = {
            "start_time": time.time(),
            "requests_received": 0,
            "flushes_completed": 0,
            "bytes_spooled": 0,
        }

    def start(self) -> Any:
        """Start daemon background flusher, IPC listener and HTTP server."""
        if self._running:
            return self._awaitable_noop()
        self._running = True
        logger.info("Starting SpoolerServer on socket: %s", self.config.ipc_socket_path)

        self._flush_thread = threading.Thread(
            target=self._flush_cycle, name="SpoolerFlusher", daemon=True
        )
        self._flush_thread.start()

        self._ipc_thread = threading.Thread(
            target=self._run_ipc_listener, name="SpoolerIPC", daemon=True
        )
        self._ipc_thread.start()

        if self.http_server:
            try:
                self.http_server.start()
            except Exception as exc:
                logger.warning("Could not start http_server: %s", exc)

        return self._awaitable_noop()

    def _awaitable_noop(self) -> Any:
        class _Awaitable:
            def __await__(self):
                async def _noop():
                    return None
                return _noop().__await__()
        return _Awaitable()

    def stop(self) -> Any:
        """Gracefully stop daemon, flush remaining tasks and close sockets."""
        if not self._running:
            return self._awaitable_noop()
        logger.info("Stopping SpoolerServer...")
        self._running = False
        self._flush_trigger.set()

        if self.http_server:
            try:
                self.http_server.stop()
            except Exception:
                pass

        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

        if self._ipc_thread and self._ipc_thread.is_alive():
            self._ipc_thread.join(timeout=2.0)

        # Final synchronous flush of pending items
        logger.info("Performing final queue flush before shutdown...")
        self.flush_now()

        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=3.0)

        if self.read_cache:
            self.read_cache.clear()

        # Clean IPC socket
        if os.name != "nt" and os.path.exists(self.config.ipc_socket_path):
            try:
                os.remove(self.config.ipc_socket_path)
            except OSError:
                pass

        logger.info("SpoolerServer stopped cleanly.")
        return self._awaitable_noop()

    def trigger_flush(self) -> None:
        """Signal background flusher to process pending tasks immediately."""
        self._flush_trigger.set()

    def flush_now(self) -> int:
        """Synchronously flush all pending tasks to disk."""
        tasks = self.task_manager.drain_queue()
        if not tasks:
            return 0
        from spooler.coalescer import coalesce_tasks
        coalesced = coalesce_tasks(tasks, self.task_manager)
        sorted_tasks = self.elevator.sort(coalesced)
        flushed_count = self.flusher.flush_batch(sorted_tasks)
        self._stats["flushes_completed"] += 1
        return flushed_count

    def read_file(self, target_path: str) -> Tuple[Optional[bytes], str]:
        """
        Read file data respecting Read-Your-Own-Writes and LRU caching:
        1. Check pending write queue in RAM (0.02ms latency, consistency guarantee)
        2. Check RAM LRU cache
        3. Read from physical disk (HDD) and populate RAM cache
        """
        norm_path = os.path.normpath(os.path.abspath(target_path))

        # 1. Read-Your-Own-Writes: check pending RAM buffer (queue or in-flight flush)
        pending_data = self.task_manager.get_pending_data(norm_path)
        if pending_data is not None:
            with self.task_manager.lock:
                is_in_flight = any(
                    os.path.abspath(t.target_path) == norm_path
                    for t in self.task_manager.in_flight_tasks.values()
                )
            source = "in_flight" if is_in_flight else "queue"
            return pending_data, source

        # 2. Check RAM Read Cache
        if self.read_cache:
            cached_data = self.read_cache.get(norm_path)
            if cached_data is not None:
                return cached_data, "ram_cache"

        # 3. Read from physical disk
        if not os.path.exists(norm_path):
            return None, "not_found"

        try:
            with open(norm_path, "rb") as f:
                disk_data = f.read()

            if self.read_cache:
                self.read_cache.put(norm_path, disk_data)

            return disk_data, "disk"
        except Exception as exc:
            logger.error("Failed to read disk file %s: %s", norm_path, exc)
            return None, "error"

    def _flush_cycle(self) -> None:
        """Flusher worker loop governed by interval or explicit triggers."""
        while self._running:
            self._flush_trigger.wait(timeout=self.config.flush_interval_seconds)
            self._flush_trigger.clear()

            if not self._running:
                break

            try:
                tasks = self.task_manager.drain_queue(max_items=getattr(self.config, "max_batch_items", 500))
                if tasks:
                    from spooler.coalescer import coalesce_tasks
                    coalesced = coalesce_tasks(tasks, self.task_manager)
                    sorted_tasks = self.elevator.sort(coalesced)
                    flushed = self.flusher.flush_batch(sorted_tasks)
                    if flushed > 0:
                        self._stats["flushes_completed"] += 1
            except Exception as exc:
                logger.error("Error during flush cycle: %s", exc, exc_info=True)

    def _run_ipc_listener(self) -> None:
        """Handle Unix domain socket / TCP IPC connections."""
        if os.name == "nt":
            self._run_tcp_listener()
        else:
            self._run_unix_listener()

    def _run_unix_listener(self) -> None:
        raw_path = str(self.config.ipc_socket_path or self.config.socket_path)
        socket_path = os.path.normpath(raw_path)
        socket_dir = os.path.dirname(socket_path)
        if socket_dir and not os.path.exists(socket_dir):
            try:
                os.makedirs(socket_dir, exist_ok=True)
            except Exception as e:
                logger.warning(f"Could not create socket dir {socket_dir}: {e}")

        if os.path.exists(socket_path):
            try:
                os.remove(socket_path)
            except OSError:
                pass

        try:
            self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._server_sock.bind(socket_path)
        except OSError as e:
            logger.warning(f"Failed to bind socket at {socket_path} ({e}), falling back to /dev/shm/ram-spooler.sock")
            socket_path = "/dev/shm/ram-spooler.sock"
            if os.path.exists(socket_path):
                try:
                    os.remove(socket_path)
                except OSError:
                    pass
            self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._server_sock.bind(socket_path)

        try:
            os.chmod(socket_path, 0o666)
        except Exception:
            pass

        # Try creating convenience symlink at /run/ram-spooler.sock if running elsewhere
        if socket_path != "/run/ram-spooler.sock":
            try:
                if os.path.islink("/run/ram-spooler.sock") or os.path.exists("/run/ram-spooler.sock"):
                    try:
                        os.unlink("/run/ram-spooler.sock")
                    except OSError:
                        pass
                os.symlink(socket_path, "/run/ram-spooler.sock")
            except Exception:
                pass

        self._server_sock.listen(128)
        self._server_sock.settimeout(1.0)
        logger.info("IPC Unix socket listening on %s", socket_path)

        while self._running:
            try:
                conn, _ = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            client_thread = threading.Thread(
                target=self._handle_client, args=(conn,), daemon=True
            )
            client_thread.start()

    def _run_tcp_listener(self) -> None:
        port = self.config.ipc_fallback_port
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(("127.0.0.1", port))
        self._server_sock.listen(128)
        self._server_sock.settimeout(1.0)
        logger.info("IPC TCP socket listening on 127.0.0.1:%d", port)

        while self._running:
            try:
                conn, _ = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            client_thread = threading.Thread(
                target=self._handle_client, args=(conn,), daemon=True
            )
            client_thread.start()

    def _handle_client(self, conn: socket.socket) -> None:
        """Read newline-delimited JSON commands and return responses."""
        with conn:
            buffer = b""
            while self._running:
                try:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buffer += chunk
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        response_dict = self._process_request_line(line)
                        resp_bytes = json.dumps(response_dict).encode("utf-8") + b"\n"
                        conn.sendall(resp_bytes)
                except Exception as exc:
                    logger.debug("IPC client connection error: %s", exc)
                    break

    def _process_request_line(self, line: bytes) -> Dict[str, Any]:
        """Dispatch a single JSON-encoded command."""
        self._stats["requests_received"] += 1
        try:
            req = json.loads(line.decode("utf-8"))
        except Exception as exc:
            return {"status": "error", "message": f"Malformed JSON: {exc}"}
        return self._dispatch_command(req)

    def _dispatch_command(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Route parsed request to corresponding subsystem."""
        cmd = req.get("cmd", "").lower()

        if cmd in ("spool", "write", "append", "submit"):
            target_path = req.get("target_path") or req.get("path")
            op_raw = req.get("op_type") or req.get("op") or "ATOMIC_WRITE"
            op_type = "ATOMIC_WRITE" if op_raw.upper() == "WRITE" else op_raw.upper()
            if not target_path:
                return {"status": "error", "message": "Missing 'target_path'"}

            payload: bytes = b""
            if "data_b64" in req:
                payload = base64.b64decode(req["data_b64"])
            elif "text" in req:
                payload = req["text"].encode("utf-8")
            elif "data" in req:
                d = req["data"]
                payload = d.encode("utf-8") if isinstance(d, str) else bytes(d)

            norm_p = os.path.normpath(os.path.abspath(target_path))

            task = self.task_manager.create_task(
                target_path=norm_p,
                op_type=op_type,
                data=payload,
            )

            # Invalidate read cache since file content is updated
            if self.read_cache:
                self.read_cache.invalidate(norm_p)

            self._stats["bytes_spooled"] += len(payload)

            if len(self.task_manager.get_active_paths()) >= self.config.max_coalesce_tasks:
                self._flush_trigger.set()

            return {
                "status": "ok",
                "task_id": task.task_id,
                "state": task.state.value,
                "target_path": norm_p,
                "bytes": len(payload),
            }

        elif cmd in ("read", "get_file"):
            target_path = req.get("target_path") or req.get("path")
            if not target_path:
                return {"status": "error", "message": "Missing 'target_path' or 'path'"}

            data, source = self.read_file(str(target_path))
            if data is None:
                if source == "not_found":
                    return {
                        "status": "error",
                        "message": f"File not found: {target_path}",
                        "source": source,
                    }
                return {
                    "status": "error",
                    "message": f"Failed to read file: {target_path}",
                    "source": source,
                }

            as_text = req.get("as_text", False)
            res: Dict[str, Any] = {
                "status": "ok",
                "target_path": target_path,
                "source": source,
                "size": len(data),
            }
            if as_text:
                try:
                    res["text"] = data.decode("utf-8")
                except UnicodeDecodeError:
                    res["data_b64"] = base64.b64encode(data).decode("ascii")
            else:
                res["data_b64"] = base64.b64encode(data).decode("ascii")
            return res

        elif cmd in ("flush", "sync"):
            flushed_count = self.flush_now()
            return {"status": "ok", "flushed_tasks": flushed_count}

        elif cmd in ("status", "stats"):
            queue_stats = self.task_manager.get_stats()
            read_cache_stats = self.read_cache.stats if self.read_cache else None
            return {
                "status": "ok",
                "uptime_seconds": time.time() - self._stats["start_time"],
                "requests_received": self._stats["requests_received"],
                "flushes_completed": self._stats["flushes_completed"],
                "bytes_spooled": self._stats["bytes_spooled"],
                "queue": queue_stats,
                "read_cache": read_cache_stats,
            }

        return {"status": "error", "message": f"Unknown command: '{cmd}'"}

    def get_status(self) -> Dict[str, Any]:
        return self._dispatch_command({"cmd": "status"})

    def read(self, target_path: str) -> Optional[bytes]:
        return self.read_file(target_path)


def run_server(config: Optional[SpoolerConfig] = None) -> None:
    """Run spooler server and block until interrupted."""
    server = SpoolerServer(config or SpoolerConfig())
    server.start()
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        server.stop()
