from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import json
import logging
from typing import Optional

from config import KAFKA_BROKER, TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_producer(broker: str = KAFKA_BROKER) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def send_message(
    producer: KafkaProducer,
    topic: str,
    message: dict,
    key: Optional[str] = None,
) -> None:
    future = producer.send(topic, value=message, key=key)
    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Sent to %s [partition %d | offset %d] key=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
            key or "None",
        )
    except KafkaError as exc:
        logger.error("Failed to send message: %s", exc)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer")
    parser.add_argument(
        "--key", default=None, help="Partition key (e.g. user-123). Omit for round-robin."
    )
    args = parser.parse_args()

    producer = create_producer()
    try:
        for i in range(5):
            send_message(producer, TOPIC, {"index": i, "msg": f"hello-{i}"}, key=args.key)
    finally:
        producer.flush()
        producer.close()
