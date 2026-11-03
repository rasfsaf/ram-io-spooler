#!/usr/bin/env python3
"""
RAM Write-Behind Spooler - Python Client Usage Example
"""

import json
import time
from spooler.client import SpoolerClient

def main():
    # Connect via Unix Domain Socket or TCP fallback
    # client = SpoolerClient(socket_path="/run/ram-spooler.sock")
    client = SpoolerClient(base_url="http://127.0.0.1:8920")

    print("[1] Spooling ATOMIC_WRITE (State / Config / JSON)...")
    for i in range(10):
        # Even if called 10 times in 100ms, only the last state is physically written to HDD
        client.write(
            target_path="/tmp/spooler_demo_state.json",
            data=json.dumps({"iteration": i, "timestamp": time.time(), "status": "active"}).encode("utf-8"),
            op_type="ATOMIC_WRITE"
        )
    print(" -> 10 state updates queued in RAM (< 0.05ms each, coalesced)")

    print("\n[2] Spooling APPEND (Logs / Metrics)...")
    for i in range(5):
        client.append(
            target_path="/tmp/spooler_demo_app.log",
            data=f"[INFO] Batch metric event #{i}\n"
        )
    print(" -> 5 log lines queued in RAM buffer")

    print("\n[3] Spooling SQLITE_SYNC (Database snapshot)...")
    sample_db_payload = b"SQLite format 3\x00SAMPLE_SNAPSHOT_DATA"
    client.sqlite_sync(
        target_path="/tmp/spooler_demo_data.db",
        data=sample_db_payload
    )
    print(" -> SQLite snapshot queued")

    print("\n[4] Querying spooler stats from RAM...")
    stats = client.stats()
    print(f" -> Queue size: {stats.get('queue_size')}, Operations: {stats.get('stats', {})}")

    print("\n[5] Forcing immediate disk flush...")
    client.flush()
    print(" -> Flush command sent! Data written to disk via elevator sorting and evicted from RAM.")

if __name__ == "__main__":
    main()
