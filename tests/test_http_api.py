"""Unit tests for RAM Spooler HTTP API."""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
import urllib.request
import urllib.error

from spooler.config import Config
from spooler.server import SpoolerServer


class TestHttpApi(unittest.IsolatedAsyncioTestCase):
    """Test HTTP API server endpoints."""

    async def asyncSetUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="http_spool_test_")
        self.config = Config(
            staging_dir=self.test_dir,
            socket_path=os.path.join(self.test_dir, "test.sock"),
            flush_interval_sec=0.2,
            max_batch_items=10,
            http_enabled=True,
            http_host="127.0.0.1",
            http_port=0,
        )
        self.server = SpoolerServer(self.config)
        await self.server.start()
        self.port = self.server.http_server.bound_port

    async def asyncTearDown(self) -> None:
        await self.server.stop()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _sync_post(self, path: str, payload: dict) -> dict:
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _sync_get(self, path: str) -> dict:
        url = f"http://127.0.0.1:{self.port}{path}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    async def _post(self, path: str, payload: dict) -> dict:
        return await asyncio.to_thread(self._sync_post, path, payload)

    async def _get(self, path: str) -> dict:
        return await asyncio.to_thread(self._sync_get, path)

    async def test_ping(self) -> None:
        resp = await self._get("/api/v1/ping")
        self.assertEqual(resp.get("status"), "ok")
        self.assertEqual(resp.get("message"), "pong")

    async def test_stats(self) -> None:
        resp = await self._get("/api/v1/stats")
        self.assertEqual(resp.get("status"), "ok")
        self.assertIn("stats", resp)

    async def test_http_write_flow(self) -> None:
        target_path = os.path.join(self.test_dir, "test_output.json")
        write_payload = {
            "path": target_path,
            "data": "{\"metric\": \"ram_speed\", \"value\": 42}",
            "op": "write"
        }
        res = await self._post("/api/v1/write", write_payload)
        self.assertEqual(res.get("status"), "ok")
        task_id = res.get("task_id")
        self.assertTrue(task_id)

        # Check status endpoint
        status_res = await self._get(f"/api/v1/status?task_id={task_id}")
        self.assertEqual(status_res.get("status"), "ok")
        self.assertIn("task_status", status_res)

        # Wait for flush cycle
        await asyncio.sleep(0.6)
        self.assertTrue(os.path.exists(target_path))
        with open(target_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("ram_speed", content)


if __name__ == "__main__":
    unittest.main()
