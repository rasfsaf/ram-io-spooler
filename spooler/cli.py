"""Command-line interface for ram-io-spooler."""

import argparse
import asyncio
import json
import os
import sys

from .client import SpoolerClient
from .config import SpoolerConfig
from .server import SpoolerServer, run_server


def format_bytes(num_bytes: int) -> str:
    """Format bytes into human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} TB"


def cmd_status(client: SpoolerClient) -> int:
    try:
        res = client.status()
        if res.get("status") != "ok":
            print(f"Error: {res.get('message')}")
            return 1

        metrics = res.get("metrics", {})
        q_len = res.get("queue_length", 0)

        print("\n=== RAM-IO-SPOOLER STATUS ===")
        print(f"Active Queue Items : {q_len}")
        print(f"Total Enqueued     : {metrics.get('total_queued', 0)}")
        print(f"Total Coalesced    : {metrics.get('total_coalesced', 0)} (IOPS saved!)")
        print(f"Total Synced (HDD) : {metrics.get('total_synced', 0)}")
        print(f"Total Evicted (RAM): {metrics.get('total_evicted', 0)}")
        print(f"Total Failed       : {metrics.get('total_failed', 0)}")
        print(f"Data Received      : {format_bytes(metrics.get('total_bytes_received', 0))}")
        print(f"Data Written to HDD: {format_bytes(metrics.get('total_bytes_written', 0))}")
        print(f"Active Staging RAM : {format_bytes(metrics.get('staging_bytes_current', 0))}")
        print("=============================\n")
        return 0
    except Exception as exc:
        print(f"Failed to communicate with spooler daemon: {exc}")
        return 1


def cmd_write(client: SpoolerClient, args: argparse.Namespace) -> int:
    target_path = args.path
    if args.file:
        with open(args.file, "rb") as f:
            data = f.read()
    elif args.data is not None:
        data = args.data.encode("utf-8")
    else:
        print("Error: Specify either --data or --file")
        return 1

    try:
        if args.append:
            res = client.append(target_path, data)
        else:
            res = client.write(target_path, data)

        if res.get("status") == "ok":
            print(f"[OK] Task {res.get('task_id')[:8]} enqueued -> {target_path} ({res.get('byte_size')} bytes, status: {res.get('task_status')})")
            return 0
        else:
            print(f"Error: {res.get('message')}")
            return 1
    except Exception as exc:
        print(f"Failed to write: {exc}")
        return 1


def cmd_flush(client: SpoolerClient) -> int:
    try:
        res = client.flush()
        if res.get("status") == "ok":
            print(f"[OK] Immediate flush complete. Flushed {res.get('flushed_count')} items to HDD.")
            return 0
        else:
            print(f"Error: {res.get('message')}")
            return 1
    except Exception as exc:
        print(f"Failed to flush: {exc}")
        return 1


def cmd_history(client: SpoolerClient, args: argparse.Namespace) -> int:
    try:
        res = client.history(limit=args.limit)
        if res.get("status") == "ok":
            history = res.get("history", [])
            print(f"\nLast {len(history)} Completed Operations:")
            for item in history:
                status = item.get("status")
                path = os.path.basename(item.get("target_path", ""))
                size = format_bytes(item.get("byte_size", 0))
                coal = f" (coalesced into {item['coalesced_into'][:8]})" if item.get("coalesced_into") else ""
                print(f" - [{status:<9}] {item.get('task_id', '')[:8]} | {item.get('op_type'):<12} | {size:>8} | {path}{coal}")
            print()
            return 0
        else:
            print(f"Error: {res.get('message')}")
            return 1
    except Exception as exc:
        print(f"Failed to fetch history: {exc}")
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="ram-io-spooler management CLI")
    subparsers = parser.add_subparsers(dest="command")

    # server
    subparsers.add_parser("server", help="Start the spooler daemon in foreground")

    # status
    subparsers.add_parser("status", help="Get spooler status and metrics")

    # flush
    subparsers.add_parser("flush", help="Force flush all queued tasks to HDD")

    # write
    p_write = subparsers.add_parser("write", help="Enqueue write to target path")
    p_write.add_argument("--path", "-p", required=True, help="Target file path on disk")
    p_write.add_argument("--data", "-d", help="Text data to write")
    p_write.add_argument("--file", "-f", help="File to read data from")
    p_write.add_argument("--append", "-a", action="store_true", help="Append instead of atomic replace")

    # history
    p_hist = subparsers.add_parser("history", help="Show recent task history")
    p_hist.add_argument("--limit", "-n", type=int, default=20, help="Number of records to show")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "server":
        asyncio.run(run_server())
        return 0

    client = SpoolerClient()

    if args.command == "status":
        return cmd_status(client)
    elif args.command == "write":
        return cmd_write(client, args)
    elif args.command == "flush":
        return cmd_flush(client)
    elif args.command == "history":
        return cmd_history(client, args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
