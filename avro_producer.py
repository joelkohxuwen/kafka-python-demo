"""Avro producer — registers schema, serialises messages with Confluent wire format.

Confluent wire format:
    [0x00]  magic byte        (1 byte)
    [    ]  schema ID         (4 bytes, big-endian int)
    [    ]  Avro-encoded data (remaining bytes)

This format lets every consumer look up the exact schema used to encode
each message, even if the schema has since been updated.
"""
import argparse
import io
import json
import logging
import struct
from datetime import datetime, timezone

import fastavro
from kafka import KafkaProducer

import schema_registry
from config import KAFKA_BROKER, TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUBJECT = f"{TOPIC}-value"   # Schema Registry convention: <topic>-value
MAGIC_BYTE = b"\x00"


def load_schema(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def serialize(schema: dict, schema_id: int, record: dict) -> bytes:
    """Encode record as Confluent wire format bytes."""
    parsed = fastavro.parse_schema(schema)
    buf = io.BytesIO()
    buf.write(MAGIC_BYTE)
    buf.write(struct.pack(">I", schema_id))   # 4-byte big-endian schema ID
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()


def create_producer(broker: str = KAFKA_BROKER) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        api_version=(2, 5, 0),   # Fixes fd=-1 on Windows (see CLAUDE.md)
        value_serializer=lambda v: v,  # raw bytes — serialisation done manually
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Avro producer with Schema Registry")
    parser.add_argument(
        "--schema",
        default="schemas/demo_message_v1.avsc",
        help="Path to .avsc schema file (default: v1)",
    )
    args = parser.parse_args()

    schema = load_schema(args.schema)
    logger.info("Checking compatibility before registering...")

    is_compatible = schema_registry.check_compatibility(SUBJECT, schema)
    if not is_compatible:
        logger.error(
            "Schema in '%s' is INCOMPATIBLE with the current version in the registry. "
            "Aborting — no messages sent.",
            args.schema,
        )
        raise SystemExit(1)

    schema_id = schema_registry.register_schema(SUBJECT, schema)
    logger.info("Schema registered under subject '%s' with ID %d", SUBJECT, schema_id)

    producer = create_producer()
    try:
        for i in range(5):
            record = {"index": i, "msg": f"hello-{i}"}

            # Add v2 fields if the schema supports them
            if "timestamp" in {f["name"] for f in schema.get("fields", [])}:
                record["timestamp"] = datetime.now(timezone.utc).isoformat()
                record["schema_version"] = 2

            payload = serialize(schema, schema_id, record)
            future = producer.send(TOPIC, value=payload)
            meta = future.get(timeout=10)
            logger.info(
                "Sent Avro record [partition %d | offset %d] schema_id=%d record=%s",
                meta.partition, meta.offset, schema_id, record,
            )
    finally:
        producer.flush()
        producer.close()
