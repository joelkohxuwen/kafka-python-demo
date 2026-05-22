from kafka import KafkaConsumer
import argparse
import json
import logging

from config import KAFKA_BROKER, TOPIC, GROUP_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_consumer(
    topic: str = TOPIC,
    broker: str = KAFKA_BROKER,
    group_id: str = GROUP_ID,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version=(2, 5, 0),  # Fixes "Invalid file descriptor: -1" on Windows
    )


def consume(consumer: KafkaConsumer) -> None:
    logger.info("Listening on topic '%s'...", TOPIC)
    for message in consumer:
        logger.info(
            "[partition %d | offset %d] %s",
            message.partition,
            message.offset,
            message.value,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer")
    parser.add_argument(
        "--group", default=GROUP_ID, help="Consumer group ID (default: %(default)s)"
    )
    args = parser.parse_args()

    logger.info("Starting consumer in group '%s'", args.group)
    consumer = create_consumer(group_id=args.group)
    try:
        consume(consumer)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer.")
    finally:
        consumer.close()
