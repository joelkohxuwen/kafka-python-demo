from kafka import KafkaConsumer
from kafka import ConsumerRebalanceListener
import argparse
import json
import logging
from typing import Callable, Optional

from config import KAFKA_BROKER, TOPIC, GROUP_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SeekListener(ConsumerRebalanceListener):
    """Runs a seek callback immediately after partitions are assigned."""

    def __init__(self, consumer: KafkaConsumer, on_assign: Optional[Callable]):
        self._consumer = consumer
        self._on_assign = on_assign

    def on_partitions_assigned(self, assigned):
        if self._on_assign:
            self._on_assign(self._consumer, assigned)

    def on_partitions_revoked(self, revoked):
        pass  # nothing to do on revoke


def create_consumer(
    topic: str = TOPIC,
    broker: str = KAFKA_BROKER,
    group_id: str = GROUP_ID,
    on_assign: Optional[Callable] = None,
) -> KafkaConsumer:
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version=(2, 5, 0),  # Fixes "Invalid file descriptor: -1" on Windows
    )
    listener = SeekListener(consumer, on_assign) if on_assign else None
    consumer.subscribe([topic], listener=listener)
    return consumer


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
    args = parser.parse_args()

    def on_assign(consumer, partitions):
        if args.from_beginning:
            logger.info("Seeking to beginning on %d partition(s)", len(partitions))
            consumer.seek_to_beginning(*partitions)
        elif args.seek_to is not None:
            logger.info(
                "Seeking to offset %d on %d partition(s)", args.seek_to, len(partitions)
            )
            for tp in partitions:
                consumer.seek(tp, args.seek_to)

    logger.info("Starting consumer in group '%s'", args.group)
    consumer = create_consumer(group_id=args.group, on_assign=on_assign)
    try:
        consume(consumer)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer.")
    finally:
        consumer.close()
