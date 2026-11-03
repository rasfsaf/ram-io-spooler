"""Asyncio UNIX socket server and background batch flusher for ram-io-spooler."""

import asyncio
import base64
import json
import logging
import os
import signal
import sys
import time
from typing import Optional

from .coalescer import coalesce_tasks
from .config import SpoolerConfig
from .elevator import elevator_sort
from .flusher import PhysicalFlusher
from .http_server import HttpServer
from .state import OpType, SpoolTask, TaskManager, TaskStatus

logger = logging.getLogger("ram_io_spooler.server")


class SpoolerServer:
    """Core daemon managing memory queue, coalescing, and background flush cycle."""

    def __init__(self, config: Optional[SpoolerConfig] = None) -> None:
        self.config = config or SpoolerConfig()
        self.task_manager = TaskManager(self.config.history_limit)
        self.flusher = PhysicalFlusher(self.task_manager)
        self.running = False
        self._server: Optional[asyncio.Server] = None
        self._http_server: Optional[HttpServer] = None
        self._flush_trigger = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self._worker_task: Optional[asyncio.Task] = None

    @property
    def http_server(self) -> Optional[HttpServer]:
        return self._http_server

    async def start(self) -> None:
        """Initialize directories, bind socket, and spawn background workers."""
        os.makedirs(self.config.staging_dir, exist_ok=True)
        socket_dir = os.path.dirname(self.config.socket_path)
        if socket_dir:
            os.makedirs(socket_dir, exist_ok=True)
        self.running = True

        # Remove stale socket if exists
        if os.path.exists(self.config.socket_path):
            try:
                os.unlink(self.config.socket_path)
            except OSError:
                pass

        # Start UNIX socket server (or fallback to TCP if AF_UNIX is unsupported on host)
        try:
            self._server = await asyncio.start_unix_server(
                self._handle_client, path=self.config.socket_path
            )
            # Set socket permissions so standard users can write to it if needed
            try:
                os.chmod(self.config.socket_path, 0o666)
            except OSError:
                pass
            logger.info("Spooler UNIX socket bound at %s", self.config.socket_path)
        except (AttributeError, NotImplementedError, OSError):
            # Fallback for Windows or systems without AF_UNIX support
            self._server = await asyncio.start_server(
                self._handle_client, host="127.0.0.1", port=self.config.fallback_tcp_port
            )
            # Record actual port if dynamically assigned (port 0)
            sockets = self._server.sockets or []
            actual_port = sockets[0].getsockname()[1] if sockets else self.config.fallback_tcp_port
            logger.info("Spooler TCP socket bound at 127.0.0.1:%s", actual_port)

        # Start HTTP API server if enabled
        if self.config.http_enabled:
            self._http_server = HttpServer(
                server=self,
                host=self.config.http_host,
                port=self.config.http_port,
            )
            await self._http_server.start()

        # Spawn flush worker loop
        self._worker_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Graceful shutdown: flush all remaining tasks to HDD and remove socket."""
        logger.info("Shutdown requested. Flushing remaining buffers to HDD...")
        self.running = False

        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        # Stop HTTP API server
        if self._http_server:
            await self._http_server.stop()

        # Close server socket
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # Remove socket file
        if os.path.exists(self.config.socket_path):
            try:
                os.unlink(self.config.socket_path)
            except OSError:
                pass

        # Final physical flush of any remaining tasks
        await self._flush_cycle(force_all=True)
        logger.info("Final flush completed. Server stopped cleanly.")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle incoming client connection and stream JSON protocol commands."""
        try:
            while self.running and not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break

                try:
                    payload = json.loads(line.decode("utf-8").strip())
                except Exception as exc:
                    err_resp = {"status": "error", "message": f"Invalid JSON: {exc}"}
                    writer.write((json.dumps(err_resp) + "\n").encode("utf-8"))
                    await writer.drain()
                    continue

                response = await self._dispatch_command(payload)
                writer.write((json.dumps(response) + "\n").encode("utf-8"))
                await writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Client handler exception: %s", exc)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch_command(self, req: dict) -> dict:
        """Dispatch incoming JSON command."""
        cmd = req.get("cmd", "").lower()

        if cmd == "ping":
            return {"status": "ok", "message": "pong", "pong": time.time()}

        if cmd in ("write", "append"):
            target_path = req.get("target_path") or req.get("path")
            if not target_path:
                return {"status": "error", "message": "Missing 'target_path' or 'path'"}

            raw_data: bytes
            if "data_b64" in req:
                try:
                    raw_data = base64.b64decode(req["data_b64"])
                except Exception as exc:
                    return {"status": "error", "message": f"Base64 decode failed: {exc}"}
            elif "text" in req:
                raw_data = str(req["text"]).encode("utf-8")
            elif "data" in req:
                data_val = req["data"]
                if isinstance(data_val, bytes):
                    raw_data = data_val
                elif isinstance(data_val, str):
                    # Try base64 first if it looks like base64, else fallback to UTF-8 text
                    try:
                        # Only decode base64 if ascii-safe and decodable back cleanly
                        decoded = base64.b64decode(data_val, validate=True)
                        raw_data = decoded
                    except Exception:
                        raw_data = data_val.encode("utf-8")
                elif isinstance(data_val, (dict, list)):
                    raw_data = json.dumps(data_val).encode("utf-8")
                else:
                    raw_data = str(data_val).encode("utf-8")
            else:
                return {"status": "error", "message": "Missing 'data', 'data_b64' or 'text' payload"}

            op_raw = req.get("op_type") or req.get("op")
            if cmd == "append":
                op_type = OpType.APPEND
            elif op_raw:
                try:
                    op_type = OpType(str(op_raw).upper())
                except ValueError:
                    op_type = OpType.ATOMIC_WRITE
            else:
                op_type = OpType.ATOMIC_WRITE

            # Safety check: avoid overflowing staging storage
            current_staging = self.task_manager.metrics.staging_bytes_current
            if current_staging + len(raw_data) > self.config.max_staging_bytes:
                # Force an immediate flush if buffer is filling up
                self._flush_trigger.set()
                return {
                    "status": "error",
                    "message": "RAM staging capacity reached. Flushing in progress. Retry shortly.",
                }

            task = self.task_manager.create_task(
                target_path=target_path,
                data=raw_data,
                op_type=op_type,
                staging_dir=self.config.staging_dir,
            )

            # If pending items exceed threshold, trigger background flush immediately
            if self.task_manager.pending_count >= self.config.max_batch_items:
                self._flush_trigger.set()

            return {
                "status": "ok",
                "task_id": task.task_id,
                "op_type": task.op_type.value,
                "target_path": task.target_path,
                "byte_size": task.byte_size,
                "checksum": task.checksum,
                "task_status": task.status.value,
            }

        if cmd in ("get", "task_status") or (cmd in ("status", "stats") and req.get("task_id")):
            task_id = req.get("task_id", "")
            task = self.task_manager.get_task(task_id)
            if not task:
                return {"status": "error", "message": f"Task '{task_id}' not found"}
            task_dict = task.to_dict()
            res = dict(task_dict)
            res["task_status"] = task.status.value
            res["status"] = "ok"
            res["task"] = task_dict
            return res

        if cmd in ("status", "stats"):
            metrics = self.task_manager.get_metrics_dict()
            return {
                "status": "ok",
                "metrics": metrics,
                "stats": metrics,
                "queue_length": self.task_manager.pending_count,
            }

        if cmd == "flush":
            # Force immediate flush and wait for completion
            flushed_count = await self._flush_cycle(force_all=True)
            return {
                "status": "ok",
                "flushed_count": flushed_count,
                "metrics": self.task_manager.get_metrics_dict(),
            }

        if cmd == "history":
            limit = int(req.get("limit", 20))
            return {
                "status": "ok",
                "history": self.task_manager.get_recent_history(limit),
            }

        return {"status": "error", "message": f"Unknown command: '{cmd}'"}

    async def _flush_loop(self) -> None:
        """Periodic background flush worker loop."""
        while self.running:
            try:
                # Wait for timeout or explicit trigger
                try:
                    await asyncio.wait_for(
                        self._flush_trigger.wait(), timeout=self.config.flush_interval_sec
                    )
                except asyncio.TimeoutError:
                    pass

                self._flush_trigger.clear()

                if self.task_manager.pending_count > 0:
                    await self._flush_cycle(force_all=False)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in flush loop: %s", exc, exc_info=True)
                await asyncio.sleep(0.5)

    async def _flush_cycle(self, force_all: bool = False) -> int:
        """Drain queue, coalesce, sort with elevator algorithm, and flush to HDD."""
        async with self._flush_lock:
            tasks = self.task_manager.drain_queue()
            if not tasks:
                return 0

            # Step 1: Write Coalescing (Deduplication)
            coalesced = coalesce_tasks(tasks, self.task_manager)

            # Step 2: LBA / Path Elevator Sorter
            sorted_tasks = elevator_sort(coalesced)

            # Step 3: Run synchronous physical flush in thread pool to avoid blocking asyncio event loop
            loop = asyncio.get_running_loop()
            flushed_count = 0

            for task in sorted_tasks:
                success = await loop.run_in_executor(None, self.flusher.flush_task, task)
                if success:
                    flushed_count += 1

            return flushed_count


async def run_server() -> None:
    """Entry point for running the server process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    server = SpoolerServer()

    loop = asyncio.get_running_loop()

    # Register OS signals for graceful shutdown on POSIX systems
    if hasattr(signal, "SIGTERM") and sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(server.stop()))

    await server.start()

    try:
        while server.running:
            await asyncio.sleep(1.0)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(run_server())
