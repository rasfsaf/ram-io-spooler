"""High-performance Python client for ram-io-spooler."""

import base64
import json
import os
import socket
from typing import Any, Dict, Optional, Union

from .config import SpoolerConfig


class SpoolerClient:
    """Synchronous & thread-safe client for ram-io-spooler (Write + Read-Your-Own-Writes)."""

    def __init__(
        self,
        socket_path: Optional[str] = None,
        timeout: float = 5.0,
    ) -> None:
        cfg = SpoolerConfig()
        self.socket_path = socket_path or cfg.socket_path
        self.timeout = timeout
        self.use_tcp = not hasattr(socket, "AF_UNIX") or not os.path.exists(self.socket_path)

    def _connect(self) -> socket.socket:
        """Create and connect socket."""
        if hasattr(socket, "AF_UNIX") and os.path.exists(self.socket_path):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect(self.socket_path)
            return s
        else:
            # Fallback to local TCP
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect(("127.0.0.1", 49152))
            return s

    def _send_cmd(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send JSON command and read response."""
        sock = self._connect()
        try:
            req_data = (json.dumps(payload) + "\n").encode("utf-8")
            sock.sendall(req_data)

            # Read newline-delimited response
            chunks = []
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                if b"\n" in chunk:
                    break

            raw_resp = b"".join(chunks).decode("utf-8").strip()
            return json.loads(raw_resp)
        finally:
            sock.close()

    def ping(self) -> bool:
        """Health check."""
        try:
            resp = self._send_cmd({"cmd": "ping"})
            return resp.get("status") == "ok"
        except Exception:
            return False

    def write(
        self,
        target_path: str,
        data: Union[bytes, str],
        op_type: str = "ATOMIC_WRITE",
    ) -> Dict[str, Any]:
        """Enqueue atomic write of whole file to target path."""
        payload: Dict[str, Any] = {
            "cmd": "write",
            "target_path": os.path.abspath(target_path),
            "op_type": op_type,
        }
        if isinstance(data, str):
            payload["text"] = data
        else:
            payload["data"] = base64.b64encode(data).decode("ascii")

        return self._send_cmd(payload)

    def append(
        self,
        target_path: str,
        data: Union[bytes, str],
    ) -> Dict[str, Any]:
        """Enqueue append write to target path."""
        payload: Dict[str, Any] = {
            "cmd": "append",
            "target_path": os.path.abspath(target_path),
        }
        if isinstance(data, str):
            payload["text"] = data
        else:
            payload["data"] = base64.b64encode(data).decode("ascii")

        return self._send_cmd(payload)

    def sqlite_sync(
        self,
        target_path: str,
        data: bytes,
    ) -> Dict[str, Any]:
        """Enqueue SQLite DB sync/snapshot with guaranteed fsync."""
        return self.write(target_path, data, op_type="SQLITE_SYNC")

    def read(
        self,
        target_path: str,
        as_text: bool = False,
    ) -> Union[bytes, str]:
        """
        Read file contents with Read-Your-Own-Writes consistency:
        1. If file is in RAM write queue -> returned instantly from RAM.
        2. If file is in RAM read LRU cache -> returned from RAM cache.
        3. If not cached -> read from HDD and cached in RAM.
        """
        resp = self.read_with_meta(target_path, as_text=as_text)
        if resp.get("status") != "ok":
            raise FileNotFoundError(resp.get("message", f"Failed to read {target_path}"))
        
        if as_text and "text" in resp:
            return resp["text"]
        elif "data_b64" in resp:
            raw_bytes = base64.b64decode(resp["data_b64"])
            return raw_bytes.decode("utf-8") if as_text else raw_bytes
        else:
            return "" if as_text else b""

    def read_with_meta(
        self,
        target_path: str,
        as_text: bool = False,
    ) -> Dict[str, Any]:
        """Read file and return full metadata (source: 'queue', 'ram_cache', 'disk', size, timing)."""
        payload = {
            "cmd": "read",
            "target_path": os.path.abspath(target_path),
            "as_text": as_text,
        }
        return self._send_cmd(payload)

    def flush(self) -> Dict[str, Any]:
        """Force an immediate flush of all pending queued items to HDD."""
        return self._send_cmd({"cmd": "flush"})

    def status(self) -> Dict[str, Any]:
        """Fetch current spooler metrics, queue status, and read cache hit stats."""
        return self._send_cmd({"cmd": "status"})

    def history(self, limit: int = 20) -> Dict[str, Any]:
        """Fetch recent completed task history."""
        return self._send_cmd({"cmd": "history", "limit": limit})
