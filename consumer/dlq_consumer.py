"""
DLQ inspector.

Reads and pretty-prints everything on the 'orders-dlq' topic, including the
original (still Avro-encoded) payload decoded back into a dict where
possible, so you can show a marker/grader exactly which messages failed and
why during a live demo.

Usage:
    python consumer/dlq_consumer.py
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, KafkaException  # noqa: E402
from common.avro_codec import decode_dlq, decode_order  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s dlq-view  %(levelname)s %(message)s")
log = logging.getLogger("dlq-view")

DLQ_TOPIC = "orders-dlq"


def main():
    parser = argparse.ArgumentParser(description="Tail and pretty-print the orders-dlq topic.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([DLQ_TOPIC])
    log.info("Watching '%s' for dead-lettered messages... (Ctrl+C to stop)", DLQ_TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            dlq_record = decode_dlq(msg.value())
            try:
                original = decode_order(dlq_record["rawPayload"])
            except Exception:
                original = "<could not decode raw payload as an Order>"

            print("-" * 70)
            print(f"orderId      : {dlq_record['orderId']}")
            print(f"errorType    : {dlq_record['errorType']}")
            print(f"errorMessage : {dlq_record['errorMessage']}")
            print(f"retryCount   : {dlq_record['retryCount']}")
            print(f"failedAt     : {dlq_record['failedAtEpochMs']}")
            print(f"originalOrder: {original}")
    except KeyboardInterrupt:
        log.info("Stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
