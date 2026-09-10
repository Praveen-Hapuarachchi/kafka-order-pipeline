"""
Order consumer.

Reads Avro-encoded order events from the 'orders' topic and:
  1. Deserializes + validates each message.
  2. "Processes" it (here: folds it into a running average of prices, both
     overall and per-product) via a function that occasionally raises a
     simulated transient error, to exercise the retry path.
  3. Applies retry logic (bounded attempts, exponential backoff) for
     TRANSIENT failures.
  4. Routes anything that is either a validation error (PERMANENT) or that
     exhausted its retries (transient-turned-permanent) to the
     'orders-dlq' topic, with the original bytes + error metadata attached.
  5. Only commits the consumer offset once a message has been handled
     (successfully processed OR safely written to the DLQ), so nothing is
     silently dropped on a crash.

Usage:
    python consumer/consumer.py
"""
import argparse
import logging
import random
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, Producer, KafkaException  # noqa: E402
from common.avro_codec import decode_order, encode_dlq  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s consumer  %(levelname)s %(message)s")
log = logging.getLogger("consumer")

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-aggregator"

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 0.5           # doubles each attempt (exponential backoff)
TRANSIENT_FAILURE_PROBABILITY = 0.25  # simulates a flaky downstream dependency


class PermanentProcessingError(Exception):
    """Message is invalid / can never succeed -> straight to DLQ, no retries."""


class TransientProcessingError(Exception):
    """Simulated recoverable failure (e.g. a downstream service blip) -> retry."""


class RunningAggregator:
    """Maintains a running (online) average of order prices, overall and per product."""

    def __init__(self):
        self.overall_count = 0
        self.overall_sum = 0.0
        self.per_product = {}  # product -> [count, sum]

    def update(self, product: str, price: float) -> float:
        self.overall_count += 1
        self.overall_sum += price

        count, total = self.per_product.get(product, [0, 0.0])
        count += 1
        total += price
        self.per_product[product] = [count, total]

        return self.overall_sum / self.overall_count

    def product_average(self, product: str) -> float:
        count, total = self.per_product.get(product, [0, 0.0])
        return total / count if count else 0.0


def validate(order: dict) -> None:
    """Permanent-failure checks: malformed/nonsensical data that will never
    succeed no matter how many times we retry it."""
    if not order.get("orderId"):
        raise PermanentProcessingError("missing orderId")
    if not order.get("product"):
        raise PermanentProcessingError("missing product")
    price = order.get("price")
    if price is None or price <= 0:
        raise PermanentProcessingError(f"invalid price: {price!r}")


def process_order(order: dict, aggregator: RunningAggregator) -> float:
    """The 'business logic' step. Raises TransientProcessingError at random
    to simulate a flaky downstream call (e.g. a metrics/aggregation service
    timing out) -- this is what the retry logic below is protecting against."""
    if random.random() < TRANSIENT_FAILURE_PROBABILITY:
        raise TransientProcessingError("simulated downstream aggregation service timeout")
    return aggregator.update(order["product"], order["price"])


def process_with_retry(order: dict, aggregator: RunningAggregator) -> int:
    """Runs process_order with bounded exponential-backoff retries.
    Returns the number of attempts made. Raises the last TransientProcessingError
    if all retries are exhausted, or re-raises a PermanentProcessingError immediately."""
    attempt = 0
    while True:
        attempt += 1
        try:
            new_avg = process_order(order, aggregator)
            log.info(
                "Processed orderId=%s product=%s price=%.2f (attempt %d) | "
                "running avg overall=%.2f, %s avg=%.2f",
                order["orderId"], order["product"], order["price"], attempt,
                new_avg, order["product"], aggregator.product_average(order["product"]),
            )
            return attempt
        except TransientProcessingError as exc:
            if attempt >= MAX_RETRIES:
                log.warning("orderId=%s exhausted %d retries: %s", order["orderId"], attempt, exc)
                raise
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            log.warning("orderId=%s transient failure (attempt %d/%d): %s -- retrying in %.1fs",
                        order["orderId"], attempt, MAX_RETRIES, exc, backoff)
            time.sleep(backoff)


def send_to_dlq(dlq_producer: Producer, raw_value: bytes, order_id, error_type: str,
                 error_message: str, retry_count: int) -> None:
    dlq_record = {
        "orderId": order_id,
        "rawPayload": raw_value,
        "errorType": error_type,
        "errorMessage": error_message,
        "retryCount": retry_count,
        "failedAtEpochMs": int(time.time() * 1000),
    }
    dlq_producer.produce(topic=DLQ_TOPIC, value=encode_dlq(dlq_record))
    dlq_producer.poll(0)
    log.error("Routed orderId=%s to DLQ (%s): %s", order_id, error_type, error_message)


def main():
    parser = argparse.ArgumentParser(description="Consume, validate, aggregate, retry & DLQ order events.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we commit manually, only after a message is fully handled
    })
    dlq_producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    aggregator = RunningAggregator()

    consumer.subscribe([ORDERS_TOPIC])
    log.info("Subscribed to '%s', group='%s'. Waiting for messages...", ORDERS_TOPIC, GROUP_ID)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            raw_value = msg.value()
            order_id = None
            try:
                order = decode_order(raw_value)
                order_id = order.get("orderId")
                validate(order)
                process_with_retry(order, aggregator)

            except PermanentProcessingError as exc:
                send_to_dlq(dlq_producer, raw_value, order_id, "VALIDATION", str(exc), retry_count=0)

            except TransientProcessingError as exc:
                send_to_dlq(dlq_producer, raw_value, order_id, "MAX_RETRIES_EXCEEDED", str(exc),
                            retry_count=MAX_RETRIES)

            except Exception as exc:  # malformed bytes / unexpected error -> never lose the message
                send_to_dlq(dlq_producer, raw_value, order_id, "DESERIALIZATION", str(exc), retry_count=0)

            # Only commit after the message has been successfully processed
            # OR safely written to the DLQ -- never on an unhandled exception.
            consumer.commit(msg)

    except KeyboardInterrupt:
        log.info("Shutting down (Ctrl+C). Final stats: overall_count=%d, overall_avg=%.2f",
                  aggregator.overall_count,
                  (aggregator.overall_sum / aggregator.overall_count) if aggregator.overall_count else 0.0)
    finally:
        dlq_producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
