# Kafka Order Pipeline — EC8202 Assignment

A Kafka-based producer/consumer system for `order` events, using **Avro
serialization**, with **real-time aggregation**, **retry logic** for
temporary failures, and a **Dead Letter Queue (DLQ)** for messages that can
never be processed successfully.

## 1. Architecture

```
                 ┌────────────┐        orders (3 partitions)        ┌──────────────┐
  producer.py -> │   Kafka    │ ───────────────────────────────────>│  consumer.py │
 (Avro-encoded)  │   Broker   │                                     │  - validate  │
                 │            │                                     │  - retry     │
                 │            │ <───────────────────────────────────│  - aggregate │
                 └────────────┘        orders-dlq (1 partition)     └──────────────┘
                       ^                                                    |
                       └──────────── dlq_consumer.py (inspector) ──────────┘
```

- **Producer** (`producer/producer.py`) generates purchase-order events
  (`orderId`, `product`, `price`), Avro-encodes them against
  `schemas/order.avsc`, and publishes them to the `orders` topic. A small,
  configurable fraction of orders are deliberately sent with an invalid
  price so the DLQ path can be demonstrated without hand-crafting bad data.
- **Consumer** (`consumer/consumer.py`) subscribes to `orders`, decodes each
  message, validates it, processes it (folding it into a running average of
  price — overall and per product), retries **transient** failures with
  exponential backoff, and routes anything unrecoverable to `orders-dlq`
  using the `schemas/order_dlq.avsc` envelope (which preserves the original
  bytes plus error metadata for later inspection/replay).
- **DLQ inspector** (`consumer/dlq_consumer.py`) tails `orders-dlq` and
  pretty-prints each dead-lettered message and why it failed — useful for
  the live demo.

### Why Avro without a Schema Registry
The assignment only requires Avro *serialization*, not a Confluent Schema
Registry. To keep the stack simple for a live demo, the schema is embedded
locally (`schemas/order.avsc`, loaded by `common/avro_codec.py`) and both
producer and consumer agree on it directly using `fastavro`. This still
gives genuine Avro binary encoding (not JSON), and is easy to upgrade to a
Schema-Registry-backed `AvroSerializer`/`AvroDeserializer` from
`confluent-kafka` later if you want schema evolution/compatibility checks.

## 2. Two layers of "retry logic" (worth explaining to the marker)

1. **Delivery-level retries (producer -> broker):** configured directly on
   the `confluent_kafka.Producer` (`acks=all`, `enable.idempotence=True`,
   `retries=5`, `retry.backoff.ms=300`). This protects against transient
   network/broker issues while *sending* a message — librdkafka handles it
   automatically.
2. **Processing-level retries (inside the consumer):** `process_order()`
   simulates a flaky downstream dependency (e.g. an aggregation service that
   times out ~25% of the time) by raising `TransientProcessingError`.
   `process_with_retry()` retries up to `MAX_RETRIES` (3) times with
   exponential backoff (0.5s, 1s, 2s). If it still fails after 3 attempts,
   the message is routed to the DLQ with `errorType=MAX_RETRIES_EXCEEDED`.

Validation failures (missing fields, non-positive price) are treated as
**permanent** — `PermanentProcessingError` — and go straight to the DLQ with
`errorType=VALIDATION`, with **no retries**, since retrying can never fix
malformed data.

The consumer only commits a Kafka offset **after** a message has either been
processed successfully or safely written to the DLQ, so a crash mid-way
never silently loses a message.

## 3. Real-time aggregation

`RunningAggregator` keeps an **online (streaming) average** — it never
recomputes from scratch, it just updates `count`/`sum` per message, both
`overall` and per `product`. Every successfully processed message logs the
updated running average(s) to the console.

## 4. Setup

### Prerequisites
- Docker + Docker Compose
- Python 3.9+

### Steps

```bash
# 1. Start Kafka (+ Zookeeper + a web UI at http://localhost:8080)
docker compose up -d

# 2. (optional but recommended) create topics explicitly
chmod +x create_topics.sh
./create_topics.sh

# 3. Install Python dependencies
python -m venv .venv && source .venv/bin/activate   # or use conda / your usual workflow
pip install -r requirements.txt

# 4. In one terminal: start the consumer
python consumer/consumer.py

# 5. In another terminal: start the DLQ inspector
python consumer/dlq_consumer.py

# 6. In a third terminal: run the producer
python producer/producer.py --count 200 --interval 0.3 --invalid-rate 0.08
```

## 5. Live-demo script (suggested order for the presentation)

1. `docker compose up -d` — show the containers coming up.
2. Show `schemas/order.avsc` — explain the Avro schema.
3. Start `consumer.py` — explain the validate -> retry -> aggregate -> DLQ
   flow, pointing at the code.
4. Start `producer.py` with a small `--count` (e.g. 20) and default
   `--invalid-rate` — watch:
   - normal orders being aggregated (running average printed per order),
   - some orders getting a `WARNING ... transient failure ... retrying`
     log line and then succeeding,
   - a few orders (the ones with negative price) being routed straight to
     the DLQ with `errorType=VALIDATION`.
5. Show `dlq_consumer.py`'s output for the messages that failed, pointing
   out the preserved original payload + error metadata.
6. (Optional) Open `http://localhost:8080` (Kafka UI) and show the
   `orders` and `orders-dlq` topics/partitions/offsets directly in the
   broker.
7. `git log` — show the commit history as evidence of independent,
   incremental work.

## 6. Project layout

```
order-pipeline/
├── docker-compose.yml       # Kafka + Zookeeper + Kafka UI
├── create_topics.sh         # explicit topic creation (partitions)
├── requirements.txt
├── schemas/
│   ├── order.avsc           # the assignment's Order schema
│   └── order_dlq.avsc       # DLQ envelope schema
├── common/
│   └── avro_codec.py        # shared Avro encode/decode helpers
├── producer/
│   └── producer.py
└── consumer/
    ├── consumer.py          # validate -> retry -> aggregate -> DLQ
    └── dlq_consumer.py      # DLQ inspector for the demo
```

## 7. Possible extensions (if you want to go further)

- Swap the local schema files for a real Confluent Schema Registry +
  `AvroSerializer`/`AvroDeserializer`, to get schema evolution/compatibility
  checking "for free".
- Add a `--seed` flag to the producer for reproducible demo runs.
- Persist the running average to a small key-value store (e.g. SQLite) so it
  survives a consumer restart, instead of living only in memory.
- Add unit tests for `validate()`, `RunningAggregator`, and the retry/backoff
  logic (these don't need a real Kafka broker to test).
