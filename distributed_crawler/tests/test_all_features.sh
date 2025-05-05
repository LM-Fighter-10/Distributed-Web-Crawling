#!/usr/bin/env bash
# File: distributed_crawler/tests/test_all_features.sh
# Purpose: End-to-end validation of Master-Node features.
# Usage: cd Distributed-Web-Crawling/distributed_crawler/tests
#        ./test_all_features.sh

set -euo pipefail
trap 'echo "🧹 Cleaning up…"; kill "$MON_PID" >/dev/null 2>&1 || true; \
     sudo iptables -D OUTPUT -p tcp -d $ES_IP --dport $ES_PORT -j DROP >/dev/null 2>&1 || true' EXIT

# ── Derive paths ───────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"            # .../distributed_crawler
MASTER_NODE_PY="$BASE_DIR/master_node.py"      # path to master_node.py
TASKS_DIR="$BASE_DIR"                          # tasks.py lives here
MONITOR_LOG="$SCRIPT_DIR/monitor.log"
# ── Config ────────────────────────────────────────────────────────────────────
INDEXER_API_BASE="http://10.128.0.5:8000/api"
ES_IP="10.128.0.5"
ES_PORT=9200
MONGO_CMD="mongo --quiet"
DB="Crawler"
# ───────────────────────────────────────────────────────────────────────────────

echo "🔹 1) Starting heartbeat & timeout monitors (5s interval)…"
# spawn inline python monitors, redirect output to a log in this tests dir
nohup python3 <<'PYCODE' > "$MONITOR_LOG" 2>&1 &
import threading, time, sys
sys.path.insert(0, "")
from master_node import heartbeat_monitor, monitor_tasks
t1 = threading.Thread(target=heartbeat_monitor, kwargs={'interval':5}, daemon=True)
t2 = threading.Thread(target=monitor_tasks,  kwargs={'interval':5}, daemon=True)
t1.start(); t2.start()
time.sleep(300)
PYCODE
MON_PID=$!
echo "    → MON_PID=$MON_PID"
sleep 7

echo
echo "🔹 2) Testing CLI search modes…"
for MODE in match boolean phrase; do
  echo -n "   • $MODE → "
  python3 "$MASTER_NODE_PY" search -k "example" -m "$MODE" -n 2
done

echo
echo "🔹 3) Testing CLI status (crawler & indexer heartbeat)…"
STATUS=$(python3 "$MASTER_NODE_PY" status)
echo "$STATUS"
echo "$STATUS" | grep -qE "Active crawlers: [1-9]" && echo "    ✓ Crawlers OK" || { echo "    ✗ Crawlers FAIL"; exit 1; }
echo "$STATUS" | grep -q "Indexer active: True"    && echo "    ✓ Indexer OK"  || { echo "    ✗ Indexer FAIL";  exit 1; }

echo
echo "🔹 4) Testing Indexer API endpoints…"
for MODE in match boolean phrase; do
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    "${INDEXER_API_BASE}/search?query=example&mode=${MODE}")
  echo -n "   • /search?mode=$MODE → $HTTP   "
  [ "$HTTP" = "200" ] && echo "✓" || { echo "✗"; exit 1; }
done
HTTP=$(curl -s -o /dev/null -w "%{http_code}" "${INDEXER_API_BASE}/metrics")
echo -n "   • /metrics → $HTTP   "
[ "$HTTP" = "200" ] && echo "✓" || { echo "✗"; exit 1; }

echo
echo "🔹 5) Testing fault-tolerant indexing retry…"
cd "$TASKS_DIR"
python3 <<'PYCODE'
import time, sys
sys.path.insert(0, "")
from tasks import index_document
res = index_document.apply_async(args=("test-doc", {"url":"x","text":"y"}))
time.sleep(5)
print("    • state =", res.state)
PYCODE

echo
echo "🔹 6) Simulating ES DOWN (block via iptables)…"
sudo iptables -I OUTPUT -p tcp -d "$ES_IP" --dport "$ES_PORT" -j DROP
sleep 7
DOWN=$(python3 "$MASTER_NODE_PY" status)
echo "$DOWN"
echo "$DOWN" | grep -q "Indexer active: False" && echo "    ✓ DOWN detected" || { echo "    ✗ DOWN not detected"; exit 1; }

echo "🔹 Restoring ES access…"
sudo iptables -D OUTPUT -p tcp -d "$ES_IP" --dport "$ES_PORT" -j DROP
sleep 7
UP=$(python3 "$MASTER_NODE_PY" status)
echo "$UP"
echo "$UP" | grep -q "Indexer active: True" && echo "    ✓ UP detected" || { echo "    ✗ UP not detected"; exit 1; }

echo
echo "🔹 7) Testing stale-task timeout & requeue…"
$MONGO_CMD <<EOF
use $DB
db.task_status.insertOne({
  task_id: "stale-test",
  url: "http://example.com",
  depth: 1,
  politeness: 0.5,
  status: "queued",
  created_at: 0
});
EOF
sleep 12
COUNT=$($MONGO_CMD <<EOF
use $DB
db.task_status.count({ origin: "stale-test" })
EOF
)
[ "$COUNT" -ge 1 ] && echo "    ✓ requeued ($COUNT copies)" || { echo "    ✗ not requeued"; exit 1; }

echo
echo "✅ ALL TESTS PASSED! 🎉"

