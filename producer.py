from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging

from config import KAFKA_BROKER, TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_producer(broker: str = KAFKA_BROKER) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_message(producer: KafkaProducer, topic: str, message: dict) -> None:
    future = producer.send(topic, value=message)
    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Sent to %s [partition %d] at offset %d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )
    except KafkaError as exc:
        logger.error("Failed to send message: %s", exc)
        raise


if __name__ == "__main__":
    producer = create_producer()
    try:
        for i in range(5):
            send_message(producer, TOPIC, {"index": i, "msg": f"hello-{i}"})
    finally:
        producer.flush()
        producer.close()
