#!/usr/bin/env bash
# RAM Write-Behind Spooler - cURL / Bash Integration Examples

set -e

SOCK="/run/ram-spooler.sock"
URL="http://localhost"

echo "=== 1. Spooling Atomic Write (State / File) ==="
curl --unix-socket "$SOCK" "$URL/spool" \
  -H "Content-Type: application/json" \
  -d '{
    "target_path": "/tmp/curl_test_state.json",
    "op_type": "ATOMIC_WRITE",
    "text": "{\"status\":\"healthy\",\"uptime\":1200}"
  }'
echo -e "\n"

echo "=== 2. Spooling Append (Logs / Events) ==="
curl --unix-socket "$SOCK" "$URL/spool" \
  -H "Content-Type: application/json" \
  -d '{
    "target_path": "/tmp/curl_test.log",
    "op_type": "APPEND",
    "text": "[2026-11-04 02:00:00] Worker process executed successfully\n"
  }'
echo -e "\n"

echo "=== 3. Querying Spooler Statistics ==="
curl --unix-socket "$SOCK" "$URL/stats"
echo -e "\n"

echo "=== 4. Forcing Immediate HDD Flush ==="
curl --unix-socket "$SOCK" -X POST "$URL/flush"
echo -e "\n"
