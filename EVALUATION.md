# Evaluation / Live Demo Guide — Kafka Order Pipeline

This walks through starting the stack and running a live evaluation on
**Windows 11 + Docker Desktop + VS Code**, using **PowerShell** (not bash —
`create_topics.sh` won't run natively, see Step 3).

Working directory for all commands below:
```
C:\Users\hapup\OneDrive\Desktop\8th sem\EC8202 Big Data and Analytics\order-pipeline
```

---

## 0. One-time setup (skip if already done)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If PowerShell blocks the activation script with an execution-policy error, run
once (as the current user, no admin needed):
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

---

## 1. Start the Kafka stack

```powershell
docker compose up -d
docker compose ps
```

Expected (matches your last run):

| NAME | STATUS | PORTS |
|---|---|---|
| order-pipeline-kafka-1 | Up | 9092 |
| order-pipeline-zookeeper-1 | Up | 2181 |
| order-pipeline-kafka-ui-1 | Up | 8080 |

> The `version` attribute warning in `docker-compose.yml` is cosmetic —
> Compose v2 ignores it. Safe to leave, or delete the `version: "3.8"` line
> to silence it.

Sanity check broker is actually accepting connections (not just "container
up"):
```powershell
docker logs order-pipeline-kafka-1 --tail 20
```
Look for `started (kafka.server.KafkaServer)` near the end.

---

## 2. Open Kafka UI (optional but good for the demo)

Browse to **http://localhost:8080**. You should see cluster `local`
connected to `kafka:29092`. Topics will appear here once created (Step 3) or
once the producer first runs (auto-create is enabled).

---

## 3. Create topics explicitly

`create_topics.sh` is a bash script — Git Bash / WSL can run it as-is, but
**plain PowerShell cannot**. Two options:

**Option A — Git Bash** (if installed with Git for Windows):
```powershell
& "C:\Program Files\Git\bin\bash.exe" create_topics.sh
```

**Option B — PowerShell-native equivalent** (no bash needed):
```powershell
$kafkaContainer = (docker ps --filter "name=kafka" --format "{{.Names}}" |
  Where-Object { $_ -notmatch "zookeeper|ui" } | Select-Object -First 1)

docker exec -it $kafkaContainer kafka-topics `
  --bootstrap-server localhost:9092 --create --if-not-exists `
  --topic orders --partitions 3 --replication-factor 1

docker exec -it $kafkaContainer kafka-topics `
  --bootstrap-server localhost:9092 --create --if-not-exists `
  --topic orders-dlq --partitions 1 --replication-factor 1
```

Verify:
```powershell
docker exec -it $kafkaContainer kafka-topics --bootstrap-server localhost:9092 --list
```
Expect `orders` and `orders-dlq`.

---

## 4. Run the evaluation (3 terminals)

Open three PowerShell terminals in VS Code (`` Ctrl+` `` then split), activate
the venv in each (`.venv\Scripts\Activate.ps1`).

**Terminal 1 — consumer** (validate → retry → aggregate → DLQ):
```powershell
python consumer.py
```

**Terminal 2 — DLQ inspector**:
```powershell
python dlq_consumer.py
```

**Terminal 3 — producer** (start small for a clean walkthrough):
```powershell
python producer.py --count 20 --interval 0.3 --invalid-rate 0.10
```

> Note: your project root has `producer.py`, `consumer.py`, `dlq_consumer.py`
> directly (not in `producer/`/`consumer/` subfolders as the README's layout
> diagram shows) — the commands above match your actual file locations.

---

## 5. What to point at while it runs

| What you'll see in Terminal 1 | Why it matters |
|---|---|
| `Processed orderId=... running avg overall=...` | Real-time online average (`RunningAggregator.update`) — no recompute from scratch |
| `WARNING ... transient failure (attempt 1/3) ... retrying in 0.5s` | Simulated flaky downstream (`TRANSIENT_FAILURE_PROBABILITY=0.25`), exponential backoff `0.5s → 1s → 2s` |
| `ERROR ... Routed orderId=... to DLQ (VALIDATION): invalid price` | Permanent failure — negative price from `--invalid-rate`, zero retries |
| `ERROR ... Routed orderId=... to DLQ (MAX_RETRIES_EXCEEDED)` | Transient failure that exhausted all 3 attempts |

In Terminal 2 you should see the same failed orders pretty-printed with
`errorType`, `errorMessage`, `retryCount`, and the decoded `originalOrder`
(preserved Avro bytes, decoded back for inspection).

In Kafka UI (http://localhost:8080 → Topics), show:
- `orders` — 3 partitions, messages flowing in
- `orders-dlq` — 1 partition, the dead-lettered subset

---

## 6. Evidence of independent work

```powershell
git log --oneline
```

Show incremental commit history as part of the write-up/demo.

---

## 7. Shutdown

```powershell
# Ctrl+C in each of the three Python terminals first, then:
docker compose down -v
```

`-v` removes the anonymous volumes too, so the next `docker compose up -d`
starts completely fresh (matches what you already did before this run).

---

## 8. Quick troubleshooting reference

| Symptom | Likely cause | Fix |
|---|---|---|
| `docker exec` "no such container" | `$kafkaContainer` picked up the UI or zookeeper container | Re-run the `Where-Object` filter line; confirm with `docker ps` |
| Producer/consumer hang with no logs | Broker not fully started yet | `docker logs order-pipeline-kafka-1` — wait for `started (kafka.server.KafkaServer)` |
| `NoBrokersAvailable` / connection refused | Port 9092 not mapped, or stale container from a previous `down` without `-v` | `docker compose down -v` then `docker compose up -d` |
| `create_topics.sh` "not recognized" | Running in plain PowerShell, not bash | Use Option B above |
| Consumer never shows DLQ messages | `--invalid-rate` set to 0, or aggregator randomness hasn't hit a transient failure yet | Increase `--invalid-rate` or `--count` |
