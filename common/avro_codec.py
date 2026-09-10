"""
Small helper around fastavro so the producer and consumer share exactly the
same encode/decode logic. We embed the schema in the binary payload's
"envelope" implicitly by both sides agreeing on schemas/order.avsc and
schemas/order_dlq.avsc -- this avoids requiring a separate Schema Registry
service for the assignment, while still using real Avro binary
serialization (as opposed to just Avro-ish JSON).
"""
import io
import os
import fastavro

_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schemas")

ORDER_SCHEMA = fastavro.schema.load_schema(os.path.join(_SCHEMA_DIR, "order.avsc"))
ORDER_DLQ_SCHEMA = fastavro.schema.load_schema(os.path.join(_SCHEMA_DIR, "order_dlq.avsc"))


def encode(record: dict, schema) -> bytes:
    """Serialize a dict to Avro binary bytes using the given parsed schema."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def decode(raw: bytes, schema) -> dict:
    """Deserialize Avro binary bytes back into a dict using the given parsed schema."""
    buf = io.BytesIO(raw)
    return fastavro.schemaless_reader(buf, schema)


def encode_order(record: dict) -> bytes:
    return encode(record, ORDER_SCHEMA)


def decode_order(raw: bytes) -> dict:
    return decode(raw, ORDER_SCHEMA)


def encode_dlq(record: dict) -> bytes:
    return encode(record, ORDER_DLQ_SCHEMA)


def decode_dlq(raw: bytes) -> dict:
    return decode(raw, ORDER_DLQ_SCHEMA)
