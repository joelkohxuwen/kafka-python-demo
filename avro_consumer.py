"""Avro consumer — reads schema ID from each message, fetches schema from
the registry, and deserialises the Avro payload.

Because the schema ID is embedded in every message, this consumer can handle
messages encoded with any schema version — v1 and v2 side by side — without
any code changes. The registry is the single source of truth.
"""
import io
import json
import logging
import struct

import fastavro
from kafka import KafkaConsumer

import schema_registry
from config import KAFKA_BROKER, TOPIC, GROUP_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAGIC_BYTE = b"\x00"
_schema_cache: dict[int, dict] = {}   # avoid fetching the same schema repeatedly


def deserialize(data: bytes) -> tuple[int, dict]:
    """Decode Confluent wire format bytes → (schema_id, record dict)."""
    if data[0:1] != MAGIC_BYTE:
        raise ValueError(f"Unknown magic byte: {data[0]!r}. Is this an Avro message?")

    schema_id = struct.unpack(">I", data[1:5])[0]

    if schema_id not in _schema_cache:
        _schema_cache[schema_id] = schema_registry.get_schema_by_id(schema_id)

    schema = fastavro.parse_schema(_schema_cache[schema_id])
    record = fastavro.schemaless_reader(io.BytesIO(data[5:]), schema)
    return schema_id, record


def create_consumer(broker: str = KAFKA_BROKER) -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=broker,
        group_id=f"{GROUP_ID}-avro",
        auto_offset_reset="earliest",
        api_version=(2, 5, 0),   # Fixes fd=-1 on Windows (see CLAUDE.md)
        value_deserializer=lambda b: b,  # raw bytes — deserialisation done manually
    )


if __name__ == "__main__":
    consumer = create_consumer()
    logger.info("Avro consumer listening on '%s'...", TOPIC)
    try:
        for message in consumer:
            try:
                schema_id, record = deserialize(message.value)
                logger.info(
                    "[partition %d | offset %d] schema_id=%d record=%s",
                    message.partition, message.offset, schema_id, record,
                )
            except Exception as exc:
                logger.error("Failed to deserialise message: %s", exc)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        consumer.close()
