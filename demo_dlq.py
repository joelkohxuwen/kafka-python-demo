"""
Dead Letter Queue demo — runs main consumer and DLQ inspector in one process.

Why one process? kafka-python-ng has a Windows-specific fd=-1 bug that surfaces
when multiple Python processes each hold Kafka connections simultaneously.
Running both consumers in threads within a single process avoids this.

Usage:
    Terminal 1: python demo_dlq.py
    Terminal 2: python producer.py
"""
import json
import logging
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

from config import KAFKA_BROKER, TOPIC, DLQ_TOPIC, GROUP_ID
from consumer import process_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def run_main_consumer() -> None:
    """Consume demo-topic; route failures to the DLQ.

    The DLQ producer is created lazily on the first failure so the consumer
    can stabilise its single connection before a second one is opened.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version=(2, 5, 0),
        session_timeout_ms=6000,      # broker clears stale sessions in 6s (default 10s)
        heartbeat_interval_ms=2000,   # must be < session_timeout_ms / 3
    )
    dlq_producer = None  # created on first failure only
    logger.info("Main consumer listening on '%s'...", TOPIC)
    try:
        for message in consumer:
            try:
                process_message(message.value)
            except Exception as exc:
                logger.warning(
                    "Failed [partition %d | offset %d] — routing to DLQ: %s",
                    message.partition, message.offset, exc,
                )
                if dlq_producer is None:
                    logger.info("Creating DLQ producer...")
                    dlq_producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKER,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        api_version=(2, 5, 0),
                    )
                dlq_producer.send(
                    DLQ_TOPIC,
                    value={
                        "original_topic": message.topic,
                        "original_partition": message.partition,
                        "original_offset": message.offset,
                        "payload": message.value,
                        "error": str(exc),
                    },
                )
    finally:
        if dlq_producer:
            dlq_producer.flush()
            dlq_producer.close()
        consumer.close()


def run_dlq_inspector() -> None:
    """Tail the DLQ and log every failed message that arrives."""
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="dlq-inspector",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        api_version=(2, 5, 0),
        session_timeout_ms=6000,
        heartbeat_interval_ms=2000,
    )
    logger.info("DLQ inspector watching '%s'...", DLQ_TOPIC)
    try:
        for message in consumer:
            p = message.value
            logger.error(
                "DLQ | topic=%s partition=%d offset=%d | error=%s | payload=%s",
                p.get("original_topic"),
                p.get("original_partition"),
                p.get("original_offset"),
                p.get("error"),
                p.get("payload"),
            )
    finally:
        consumer.close()


def with_retry(fn, name: str, initial_delay: float = 2.0, max_delay: float = 30.0, max_retries: int = 8) -> None:
    """Run fn(), restarting on fd=-1 errors with exponential backoff.

    Gives up after max_retries so the thread doesn't spin forever when the
    broker is genuinely unreachable (e.g. stale session after a hard restart).
    """
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            fn()
            return  # fn exited cleanly
        except ValueError as exc:
            if "Invalid file descriptor" in str(exc):
                logger.warning(
                    "%s: connection reset (attempt %d/%d) — retrying in %.0fs...",
                    name, attempt, max_retries, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, max_delay)  # exponential backoff, capped at max_delay
            else:
                raise
    logger.error("%s: giving up after %d attempts.", name, max_retries)


if __name__ == "__main__":
    main_thread = threading.Thread(
        target=with_retry, args=(run_main_consumer, "main-consumer"),
        name="main-consumer", daemon=True,
    )
    dlq_thread = threading.Thread(
        target=with_retry, args=(run_dlq_inspector, "dlq-inspector"),
        name="dlq-inspector", daemon=True,
    )

    main_thread.start()
    time.sleep(3)  # let main consumer stabilise before opening a second connection
    dlq_thread.start()

    try:
        # Join with a timeout so Ctrl+C is never blocked on Windows.
        # Daemon threads are killed automatically when the main thread exits.
        while main_thread.is_alive() or dlq_thread.is_alive():
            main_thread.join(timeout=0.5)
            dlq_thread.join(timeout=0.5)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
