"""Lightweight HTTP REST API Server for RAM Write-Behind Scribe Engine.
Runs alongside the Unix socket, providing standard HTTP endpoints:
  POST /api/v1/write
  POST /api/v1/append
  GET  /api/v1/status?task_id=...
  GET  /api/v1/stats
  GET  /api/v1/ping
  POST /api/v1/sync_wait
"""

import asyncio
import json
import logging
import urllib.parse
from typing import Any, Optional

logger = logging.getLogger("spooler.http")


class HttpServer:
    """Asyncio-native HTTP/1.1 REST API server without external dependencies."""

    def __init__(self, host: str, port: int, server: Any):
        self.host = host
        self.port = port
        self.server = server
        self._srv: Optional[asyncio.AbstractServer] = None

    @property
    def bound_port(self) -> int:
        """Return actual bound port."""
        if self._srv and self._srv.sockets:
            return self._srv.sockets[0].getsockname()[1]
        return self.port

    async def start(self) -> None:
        """Start listening for HTTP requests."""
        self._srv = await asyncio.start_server(self._handle_client, self.host, self.port)
        logger.info("HTTP API listening on http://%s:%d", self.host, self.bound_port)

    async def stop(self) -> None:
        """Gracefully close HTTP server."""
        if self._srv:
            self._srv.close()
            try:
                await self._srv.wait_closed()
            except Exception:
                pass
            self._srv = None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                return

            line = request_line.decode("utf-8", errors="replace").strip()
            parts = line.split(" ")
            if len(parts) < 2:
                self._send_response(writer, 400, {"status": "error", "message": "Bad Request"})
                await writer.drain()
                writer.close()
                return

            method = parts[0].upper()
            raw_url = parts[1]

            headers = {}
            while True:
                hdr_line = await reader.readline()
                if not hdr_line or hdr_line in (b"\r\n", b"\n"):
                    break
                h_str = hdr_line.decode("utf-8", errors="replace").strip()
                if ":" in h_str:
                    hk, hv = h_str.split(":", 1)
                    headers[hk.strip().lower()] = hv.strip()

            body = b""
            content_length = int(headers.get("content-length", 0))
            if content_length > 0:
                body = await reader.readexactly(content_length)

            parsed_url = urllib.parse.urlparse(raw_url)
            path = parsed_url.path
            query_params = urllib.parse.parse_qs(parsed_url.query)

            # Route requests
            if method == "GET" and path in ("/api/v1/ping", "/ping"):
                resp = await self.server._dispatch_command({"cmd": "ping"})
                self._send_response(writer, 200, resp)

            elif method == "GET" and path in ("/api/v1/stats", "/stats"):
                resp = await self.server._dispatch_command({"cmd": "stats"})
                self._send_response(writer, 200, resp)

            elif method == "GET" and path in ("/api/v1/status", "/status"):
                task_id = query_params.get("task_id", [""])[0]
                resp = await self.server._dispatch_command({"cmd": "status", "task_id": task_id})
                self._send_response(writer, 200, resp)

            elif method == "POST" and path in ("/api/v1/write", "/write"):
                payload = json.loads(body.decode("utf-8")) if body else {}
                payload["cmd"] = "write"
                resp = await self.server._dispatch_command(payload)
                status_code = 200 if resp.get("status") == "ok" else 400
                self._send_response(writer, status_code, resp)

            elif method == "POST" and path in ("/api/v1/append", "/append"):
                payload = json.loads(body.decode("utf-8")) if body else {}
                payload["cmd"] = "append"
                resp = await self.server._dispatch_command(payload)
                status_code = 200 if resp.get("status") == "ok" else 400
                self._send_response(writer, status_code, resp)

            elif method == "POST" and path in ("/api/v1/sync_wait", "/sync_wait"):
                payload = json.loads(body.decode("utf-8")) if body else {}
                payload["cmd"] = "sync_wait"
                resp = await self.server._dispatch_command(payload)
                self._send_response(writer, 200, resp)

            else:
                self._send_response(writer, 404, {"status": "error", "message": f"Route not found: {method} {path}"})

            await writer.drain()
        except Exception as exc:
            logger.error("HTTP handler error: %s", exc)
            try:
                self._send_response(writer, 500, {"status": "error", "message": str(exc)})
                await writer.drain()
            except Exception:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _send_response(self, writer: asyncio.StreamWriter, status_code: int, data: dict) -> None:
        body_bytes = json.dumps(data, indent=2).encode("utf-8")
        reason = "OK" if status_code == 200 else ("Not Found" if status_code == 404 else "Error")
        header = (
            f"HTTP/1.1 {status_code} {reason}\r\n"
            f"Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body_bytes)}\r\n"
            f"Connection: close\r\n\r\n"
        )
        writer.write(header.encode("latin1") + body_bytes)
