"""
Order producer.

Generates purchase-order events and publishes them, Avro-encoded, to the
'orders' Kafka topic. A configurable fraction of messages are deliberately
made invalid (negative price / empty product) so that the consumer's
validation + DLQ path can be demonstrated live without hand-editing data.

Usage:
    python producer/producer.py --count 200 --interval 0.3 --invalid-rate 0.05

Kafka delivery reliability (transient network/broker failures) is handled by
librdkafka itself via the producer config below (retries, backoff, acks=all,
idempotence) -- this is the "retry logic" at the transport level. The
consumer implements a second, independent layer of retry logic for failures
that happen while *processing* a message (see consumer/consumer.py).
"""
import argparse
import logging
import random
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Producer  # noqa: E402
from common.avro_codec import encode_order  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s producer  %(levelname)s %(message)s")
log = logging.getLogger("producer")

TOPIC = "orders"
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    # --- delivery-level retry / reliability settings ---
    "acks": "all",                 # wait for all in-sync replicas
    "enable.idempotence": True,    # avoid duplicate writes on retry
    "retries": 5,                  # retry transient broker/network errors
    "retry.backoff.ms": 300,
    "linger.ms": 20,               # small batching window
}


def make_order(order_seq: int, invalid_rate: float) -> dict:
    """Build one order dict. With probability `invalid_rate`, deliberately
    produce a record that will fail the consumer's validation step, so the
    DLQ path can be demoed (e.g. a negative price)."""
    order = {
        "orderId": str(1000 + order_seq),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }
    if random.random() < invalid_rate:
        # Simulate bad upstream data -> a PERMANENT failure for the consumer.
        order["price"] = -abs(order["price"])
    return order


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        log.debug("Delivered to %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset())


def main():
    parser = argparse.ArgumentParser(description="Produce Avro-encoded order events to Kafka.")
    parser.add_argument("--bootstrap-servers", default=PRODUCER_CONFIG["bootstrap.servers"])
    parser.add_argument("--count", type=int, default=100, help="Number of orders to send")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds to sleep between sends")
    parser.add_argument("--invalid-rate", type=float, default=0.08,
                         help="Fraction (0-1) of orders sent with an invalid price, to trigger the DLQ path")
    args = parser.parse_args()

    config = dict(PRODUCER_CONFIG)
    config["bootstrap.servers"] = args.bootstrap_servers
    producer = Producer(config)

    log.info("Starting producer: %d orders -> topic '%s' (invalid-rate=%.0f%%)",
              args.count, TOPIC, args.invalid_rate * 100)

    for i in range(args.count):
        order = make_order(i, args.invalid_rate)
        payload = encode_order(order)
        producer.produce(
            topic=TOPIC,
            key=order["orderId"].encode("utf-8"),
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks
        log.info("Sent orderId=%s product=%s price=%.2f", order["orderId"], order["product"], order["price"])
        time.sleep(args.interval)

    log.info("Flushing remaining messages...")
    producer.flush(30)
    log.info("Done. %d orders sent.", args.count)


if __name__ == "__main__":
    main()
