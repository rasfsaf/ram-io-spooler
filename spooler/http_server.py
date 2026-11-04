"""
HTTP REST / Unix Socket API server for RAM IO Spooler.
Provides endpoints for write spooling, read caching (Read-Your-Own-Writes),
manual flushing, and live engine diagnostics.
"""

import json
import base64
import urllib.parse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, TYPE_CHECKING, Tuple, Dict, Any

if TYPE_CHECKING:
    from .server import SpoolerServer


class SpoolerHTTPHandler(BaseHTTPRequestHandler):
    server_instance: Optional["SpoolerServer"] = None

    def _send_json_response(self, code: int, data: Dict[str, Any], extra_headers: Optional[Dict[str, str]] = None) -> None:
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def _send_raw_response(self, code: int, data: bytes, content_type: str = "application/octet-stream", extra_headers: Optional[Dict[str, str]] = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if not self.server_instance:
            self._send_json_response(503, {"status": "error", "message": "Server instance unavailable"})
            return

        if path in ("/api/v1/ping", "/ping"):
            self._send_json_response(200, {"status": "ok", "message": "pong"})
            return

        if path in ("/api/v1/status", "/api/v1/stats", "/status", "/stats"):
            task_id = query.get("task_id", [None])[0]
            if task_id:
                task = None
                if hasattr(self.server_instance, "task_manager"):
                    task = self.server_instance.task_manager.get_task(task_id)
                elif hasattr(self.server_instance, "state"):
                    task = self.server_instance.state.get_task(task_id)
                if task:
                    task_dict = {
                        "id": getattr(task, "task_id", getattr(task, "id", task_id)),
                        "status": getattr(task.state, "value", str(task.state)) if hasattr(task, "state") else (getattr(task.status, "value", str(task.status)) if hasattr(task, "status") else "queued"),
                        "path": getattr(task, "target_path", getattr(task, "path", ""))
                    }
                    self._send_json_response(200, {
                        "status": "ok",
                        "task_status": task_dict["status"],
                        "task": task_dict
                    })
                    return
                else:
                    self._send_json_response(404, {"status": "error", "message": f"Task {task_id} not found"})
                    return
            stats = self.server_instance.get_status()
            self._send_json_response(200, {"status": "ok", "stats": stats})
            return

        if path in ("/api/v1/read", "/read"):
            target_path = query.get("path", query.get("target_path", [None]))[0]
            if not target_path:
                self._send_json_response(400, {"status": "error", "message": "Missing required query param 'path'"})
                return

            data, source = self.server_instance.read_file(target_path)
            if data is None:
                status_code = 404 if source == "not_found" else 500
                self._send_json_response(status_code, {"status": "error", "message": f"File not found or unreadable: {target_path}", "source": source})
                return

            extra_hdrs = {"X-Spooler-Source": source}
            raw_flag = query.get("raw", ["0"])[0] in ("1", "true", "yes")
            accept_hdr = self.headers.get("Accept", "")

            if raw_flag or "application/octet-stream" in accept_hdr:
                self._send_raw_response(200, data, extra_headers=extra_hdrs)
                return

            as_text_flag = query.get("as_text", ["0"])[0] in ("1", "true", "yes")
            res_json: Dict[str, Any] = {
                "status": "ok",
                "target_path": target_path,
                "source": source,
                "size": len(data),
            }
            if as_text_flag:
                try:
                    res_json["text"] = data.decode("utf-8")
                except UnicodeDecodeError:
                    res_json["data_b64"] = base64.b64encode(data).decode("ascii")
            else:
                res_json["data_b64"] = base64.b64encode(data).decode("ascii")

            self._send_json_response(200, res_json, extra_headers=extra_hdrs)
            return

        self._send_json_response(404, {"status": "error", "message": f"Endpoint not found: {path}"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if not self.server_instance:
            self._send_json_response(503, {"status": "error", "message": "Server instance unavailable"})
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b""

        if path in ("/api/v1/flush", "/flush"):
            result = self.server_instance.trigger_flush()
            self._send_json_response(200, result)
            return

        if path in ("/api/v1/read", "/read"):
            try:
                body = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
            except Exception as e:
                self._send_json_response(400, {"status": "error", "message": f"Invalid JSON body: {e}"})
                return

            target_path = body.get("path") or body.get("target_path")
            if not target_path:
                self._send_json_response(400, {"status": "error", "message": "Missing 'path' or 'target_path' in body"})
                return

            data, source = self.server_instance.read_file(target_path)
            if data is None:
                status_code = 404 if source == "not_found" else 500
                self._send_json_response(status_code, {"status": "error", "message": f"File not found: {target_path}", "source": source})
                return

            extra_hdrs = {"X-Spooler-Source": source}
            as_text = body.get("as_text", False)
            raw = body.get("raw", False)

            if raw:
                self._send_raw_response(200, data, extra_headers=extra_hdrs)
                return

            res_json = {
                "status": "ok",
                "target_path": target_path,
                "source": source,
                "size": len(data),
            }
            if as_text:
                try:
                    res_json["text"] = data.decode("utf-8")
                except UnicodeDecodeError:
                    res_json["data_b64"] = base64.b64encode(data).decode("ascii")
            else:
                res_json["data_b64"] = base64.b64encode(data).decode("ascii")

            self._send_json_response(200, res_json, extra_headers=extra_hdrs)
            return

        if path in ("/api/v1/spool", "/spool", "/api/v1/write", "/write", "/append", "/sqlite_sync"):
            try:
                body = json.loads(body_bytes.decode("utf-8"))
            except Exception as e:
                self._send_json_response(400, {"status": "error", "message": f"Invalid JSON body: {e}"})
                return

            cmd = body.get("op_type") or body.get("op") or "ATOMIC_WRITE"
            if path.endswith("/append"):
                cmd = "APPEND"
            elif path.endswith("/sqlite_sync"):
                cmd = "SQLITE_SYNC"

            body["cmd"] = "spool"
            body["op_type"] = cmd
            result = self.server_instance._dispatch_command(body)
            code = 200 if result.get("status") == "ok" else 400
            self._send_json_response(code, result)
            return

        self._send_json_response(404, {"status": "error", "message": f"Endpoint not found: {path}"})

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress noisy HTTP stdout logging in daemon mode
        pass


class SpoolerHttpServer:
    """HTTP/REST server component for Spooler daemon."""

    def __init__(self, config: Any, server_instance: Any):
        self.config = config
        self.server_instance = server_instance
        self.host = getattr(config, "http_host", "127.0.0.1")
        self.port = getattr(config, "http_port", 0)
        self.httpd = None
        self.thread = None
        self.bound_port = self.port

    def start(self) -> None:
        srv_ref = self.server_instance
        class _BoundHandler(SpoolerHTTPHandler):
            server_instance = srv_ref
        self.httpd = HTTPServer((self.host, self.port), _BoundHandler)
        self.bound_port = self.httpd.server_port
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None


SpoolerHttpHandler = SpoolerHTTPHandler
SpoolerHTTPServer = SpoolerHttpServer


SpoolerHTTPServer = SpoolerHttpServer
