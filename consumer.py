from kafka import KafkaConsumer, KafkaProducer
import argparse
import json
import logging
from typing import Optional

from config import KAFKA_BROKER, TOPIC, DLQ_TOPIC, GROUP_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_consumer(
    topic: str = TOPIC,
    broker: str = KAFKA_BROKER,
    group_id: str = GROUP_ID,
) -> KafkaConsumer:
    # Topic passed to constructor — avoids the extra subscribe() socket
    # operations that trigger "Invalid file descriptor: -1" on Windows.
    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version=(2, 5, 0),  # Fixes "Invalid file descriptor: -1" on Windows
    )


def apply_seek(consumer: KafkaConsumer, from_beginning: bool, seek_to: Optional[int]) -> None:
    """Poll once to trigger partition assignment, then seek as requested."""
    if not from_beginning and seek_to is None:
        return

    # Poll with a timeout so the broker assigns partitions to this consumer.
    consumer.poll(timeout_ms=5000)
    assigned = consumer.assignment()

    if not assigned:
        logger.warning("No partitions assigned yet — seek skipped.")
        return

    if from_beginning:
        logger.info("Seeking to beginning on %d partition(s)", len(assigned))
        consumer.seek_to_beginning(*assigned)
    elif seek_to is not None:
        logger.info("Seeking to offset %d on %d partition(s)", seek_to, len(assigned))
        for tp in assigned:
            consumer.seek(tp, seek_to)


def consume(consumer: KafkaConsumer) -> None:
    logger.info("Listening on topic '%s'...", TOPIC)
    for message in consumer:
        logger.info(
            "[partition %d | offset %d] %s",
            message.partition,
            message.offset,
            message.value,
        )


def process_message(message: dict) -> None:
    """Business logic — handles both v1 and v2 message schemas.

    v1: {"index": int, "msg": str}
    v2: {"index": int, "msg": str, "timestamp": str, "schema_version": int}

    New optional fields are read with .get() and a safe default so a v1
    consumer is never broken by a v2 producer, and vice versa.
    """
    if message.get("index", 0) % 2 != 0:
        raise ValueError(f"Simulated failure for index {message['index']}")

    schema_version = message.get("schema_version", 1)  # default to v1 if absent
    timestamp = message.get("timestamp", "n/a")        # safe default for v1 messages
    logger.info(
        "Processed OK (schema v%d): index=%d msg=%s timestamp=%s",
        schema_version,
        message.get("index"),
        message.get("msg"),
        timestamp,
    )


def process_message_strict(message: dict) -> None:
    """Simulates a developer who wrote consumer code assuming v2 fields always exist.

    This is the WRONG pattern — it uses direct key access instead of .get().
    Breaks immediately when a v1 message (no 'timestamp') arrives.
    """
    if message["index"] % 2 != 0:                 # KeyError if "index" missing
        raise ValueError(f"Simulated failure for index {message['index']}")

    timestamp = message["timestamp"]               # ← CRASH on v1 messages (KeyError)
    schema_version = message["schema_version"]     # ← CRASH on v1 messages (KeyError)
    logger.info(
        "Processed OK (schema v%d): index=%d msg=%s timestamp=%s",
        schema_version,
        message["index"],
        message["msg"],
        timestamp,
    )


def consume_with_dlq(
    consumer: KafkaConsumer,
    dlq_producer: KafkaProducer,
    dlq_topic: str = DLQ_TOPIC,
) -> None:
    """Consume messages and route failures to the dead letter queue."""
    logger.info("Listening with DLQ enabled (failures → '%s')...", dlq_topic)
    for message in consumer:
        try:
            process_message(message.value)
        except Exception as exc:
            logger.warning(
                "Processing failed [partition %d | offset %d] — routing to DLQ: %s",
                message.partition,
                message.offset,
                exc,
            )
            dlq_producer.send(
                dlq_topic,
                value={
                    "original_topic": message.topic,
                    "original_partition": message.partition,
                    "original_offset": message.offset,
                    "payload": message.value,
                    "error": str(exc),
                },
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer")
    parser.add_argument(
        "--group", default=GROUP_ID, help="Consumer group ID (default: %(default)s)"
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Ignore committed offsets and replay all messages from offset 0",
    )
    parser.add_argument(
        "--seek-to",
        type=int,
        default=None,
        metavar="OFFSET",
        help="Seek all partitions to this offset before consuming",
    )
    parser.add_argument(
        "--dlq",
        action="store_true",
        help="Enable dead letter queue — route failed messages to demo-topic-dlq",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Use strict (broken) schema handling — crashes on v1 messages to demo schema evolution failure.",
    )
    args = parser.parse_args()

    logger.info("Starting consumer in group '%s'", args.group)
    consumer = create_consumer(group_id=args.group)
    try:
        apply_seek(consumer, from_beginning=args.from_beginning, seek_to=args.seek_to)
        if args.strict:
            # Demonstrate schema evolution failure — crashes on v1 messages
            logger.warning("Running in STRICT mode — will crash on v1 messages!")
            for msg in consumer:
                process_message_strict(msg.value)
        elif args.dlq:
            dlq_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 5, 0),
            )
            try:
                consume_with_dlq(consumer, dlq_producer)
            finally:
                dlq_producer.flush()
                dlq_producer.close()
        else:
            consume(consumer)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer.")
    finally:
        consumer.close()
