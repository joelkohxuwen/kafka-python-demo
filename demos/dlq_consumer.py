"""Dead letter queue consumer — tails demo-topic-dlq and logs failed messages."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))  # make config importable

from kafka import KafkaConsumer
import json
import logging

from config import KAFKA_BROKER, DLQ_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="dlq-inspector",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version=(2, 5, 0),
    )
    logger.info("Watching dead letter queue '%s'...", DLQ_TOPIC)
    try:
        for message in consumer:
            payload = message.value
            logger.error(
                "DLQ message — original topic=%s partition=%d offset=%d | error=%s | payload=%s",
                payload.get("original_topic"),
                payload.get("original_partition"),
                payload.get("original_offset"),
                payload.get("error"),
                payload.get("payload"),
            )
    except KeyboardInterrupt:
        logger.info("DLQ consumer shutting down.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
