import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))  # make config importable

"""Idempotent producer demo — contrasts at-least-once vs exactly-once delivery.

kafka-python-ng does not implement broker-level enable_idempotence, so this
demo uses the standard production alternative: application-level idempotence
via a unique message_id embedded in every message.

PHASE 1 — Standard producer, naive consumer (at-least-once):
  Producer sends 5 messages. index=2 is double-sent to simulate a lost ack
  forcing a retry. The naive consumer has no deduplication logic — it processes
  every message it receives, including the duplicate.
  Result: 6 messages processed, 1 unwanted duplicate.

PHASE 2 — Idempotent producer, dedup consumer (exactly-once):
  Producer stamps every message with a unique message_id (UUID). On retry the
  same message_id is re-sent. The consumer maintains a seen_ids set; any message
  whose ID was already processed is silently skipped.
  Result: 5 messages processed, 0 duplicates — even with the same retry.

This pattern works with any broker or library version and is the standard
approach in production systems where exactly-once end-to-end is required.
"""
import json
import logging
import time
import uuid
from threading import Event, Thread

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import KAFKA_BROKER, GROUP_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

DEMO_TOPIC = "idempotent-demo"
BATCH_SIZE = 5


# ── Topic setup ────────────────────────────────────────────────────────────────

def ensure_topic() -> None:
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, api_version=(2, 5, 0))
        admin.create_topics([
            NewTopic(name=DEMO_TOPIC, num_partitions=1, replication_factor=1)
        ])
        logger.info("Created topic '%s'.", DEMO_TOPIC)
        admin.close()
    except TopicAlreadyExistsError:
        logger.info("Topic '%s' already exists — reusing.", DEMO_TOPIC)
    except Exception as exc:
        logger.warning("Topic creation skipped (proceeding anyway): %s", exc)


# ── Retry wrapper (Rule 6 + Rule 8) ───────────────────────────────────────────

def make_retrying_consumer(
    consumer_fn,
    group_id: str,
    results: dict,
    stop_event: Event,
) -> tuple[Thread, Event]:
    """Start a consumer thread wrapped in an fd=-1 retry loop.

    Returns (thread, ready_event).  ready_event is set by the consumer after
    its first successful poll — the caller should wait on it before sending any
    messages, so no messages are produced before the consumer is subscribed.
    """
    ready = Event()

    def target():
        delay = 2.0
        for attempt in range(1, 9):   # Rule 8: cap at 8 attempts
            try:
                consumer_fn(group_id, results, stop_event, ready)
                return
            except ValueError as exc:
                if "Invalid file descriptor" not in str(exc):
                    raise
                if stop_event.is_set():
                    return
                logger.warning(
                    "Consumer fd=-1 on attempt %d — retrying in %.0fs ...",
                    attempt, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)  # exponential back-off, cap 30s
        logger.error("Consumer: giving up after 8 attempts.")

    thread = Thread(target=target, daemon=True)
    thread.start()
    return thread, ready


# ── Producers ──────────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(2, 5, 0),   # Rule 1
        acks="all",
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_batch_no_ids(
    producer: KafkaProducer,
    run_id: str,
    simulate_retry_on: int | None = None,
) -> None:
    """Phase 1: send messages WITHOUT a message_id. Retry is indistinguishable."""
    for i in range(BATCH_SIZE):
        record = {"run_id": run_id, "index": i}
        producer.send(DEMO_TOPIC, value=record).get(timeout=10)
        logger.info("  → Sent   index=%d  (no message_id)", i)

        if i == simulate_retry_on:
            logger.warning(
                "  ↳ Lost ack simulated on index=%d — retrying same payload ...", i
            )
            time.sleep(0.1)
            producer.send(DEMO_TOPIC, value=record).get(timeout=10)
            logger.warning(
                "  ↳ Retry written. Broker can't tell it's a duplicate "
                "— both copies sit in the log."
            )

    producer.flush()
    producer.close()


def send_batch_with_ids(
    producer: KafkaProducer,
    run_id: str,
    simulate_retry_on: int | None = None,
) -> None:
    """Phase 2: stamp every message with a UUID. On retry, re-use the same UUID."""
    message_ids: list[str] = []
    for i in range(BATCH_SIZE):
        mid = str(uuid.uuid4())
        message_ids.append(mid)
        record = {"run_id": run_id, "index": i, "message_id": mid}
        producer.send(DEMO_TOPIC, value=record).get(timeout=10)
        logger.info("  → Sent   index=%d  message_id=%s", i, mid)

        if i == simulate_retry_on:
            logger.warning(
                "  ↳ Lost ack simulated on index=%d — retrying with SAME message_id ...", i
            )
            time.sleep(0.1)
            producer.send(DEMO_TOPIC, value=record).get(timeout=10)   # same UUID
            logger.warning(
                "  ↳ Retry written with same message_id — "
                "consumer will recognise and skip it."
            )

    producer.flush()
    producer.close()


# ── Consumers ──────────────────────────────────────────────────────────────────

def run_naive_consumer(
    group_id: str, results: dict, stop_event: Event, ready_event: Event
) -> None:
    """Phase 1: no dedup logic — processes every message, including duplicates."""
    consumer = KafkaConsumer(
        DEMO_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        auto_offset_reset="latest",
        api_version=(2, 5, 0),        # Rule 1
        session_timeout_ms=6000,      # Rule 7
        heartbeat_interval_ms=2000,   # Rule 7
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    total = 0
    processed = 0
    try:
        while not stop_event.is_set():
            batch = consumer.poll(timeout_ms=500)
            ready_event.set()   # first successful poll → consumer is live at "latest"
            for _, messages in batch.items():
                for msg in messages:
                    total += 1
                    processed += 1
                    logger.info(
                        "  ✓  PROCESSED  index=%d  (offset=%d)"
                        "  ← no dedup, every message is acted on",
                        msg.value.get("index"), msg.offset,
                    )
    finally:
        results["total"] = results.get("total", 0) + total
        results["processed"] = results.get("processed", 0) + processed
        consumer.close()


def run_dedup_consumer(
    group_id: str, results: dict, stop_event: Event, ready_event: Event
) -> None:
    """Phase 2: checks message_id before processing — skips anything already seen."""
    consumer = KafkaConsumer(
        DEMO_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        auto_offset_reset="latest",
        api_version=(2, 5, 0),        # Rule 1
        session_timeout_ms=6000,      # Rule 7
        heartbeat_interval_ms=2000,   # Rule 7
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    seen_ids: set[str] = set()
    total = 0
    processed = 0
    skipped = 0
    try:
        while not stop_event.is_set():
            batch = consumer.poll(timeout_ms=500)
            ready_event.set()   # first successful poll → consumer is live at "latest"
            for _, messages in batch.items():
                for msg in messages:
                    total += 1
                    mid = msg.value.get("message_id")
                    if mid in seen_ids:
                        skipped += 1
                        logger.warning(
                            "  ⊘  SKIPPED    index=%d  message_id=%s"
                            "  (already processed)",
                            msg.value.get("index"), mid,
                        )
                    else:
                        processed += 1
                        seen_ids.add(mid)
                        logger.info(
                            "  ✓  PROCESSED  index=%d  message_id=%s",
                            msg.value.get("index"), mid,
                        )
    finally:
        results["total"] = results.get("total", 0) + total
        results["processed"] = results.get("processed", 0) + processed
        results["skipped"] = results.get("skipped", 0) + skipped
        consumer.close()


# ── Phase runner ───────────────────────────────────────────────────────────────

def run_phase(
    group_suffix: str,
    consumer_fn,
    producer_send_fn,
    run_id: str,
    simulate_retry_on: int | None,
) -> dict:
    results: dict = {}
    stop = Event()

    thread, ready = make_retrying_consumer(consumer_fn, f"{GROUP_ID}-idempotent-{group_suffix}", results, stop)

    logger.info("  Waiting for consumer to connect...")
    if not ready.wait(timeout=15):
        logger.error("Consumer did not start within 15 s — aborting phase.")
        stop.set()
        while thread.is_alive():
            thread.join(timeout=0.5)
        return results

    logger.info("  Consumer ready. Starting producer.")
    producer = create_producer()
    producer_send_fn(producer, run_id, simulate_retry_on)

    time.sleep(2)   # let consumer drain the final messages
    stop.set()
    while thread.is_alive():   # Rule 9: always join with timeout
        thread.join(timeout=0.5)

    return results


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ensure_topic()
    time.sleep(2)   # let admin client connections fully close before consumers start

    # ── Phase 1: no message IDs, naive consumer ────────────────────────────────
    logger.info("")
    logger.info("═" * 62)
    logger.info("PHASE 1 — No message IDs  (at-least-once / naive consumer)")
    logger.info("Sending %d messages. index=2 double-sent to simulate lost ack.", BATCH_SIZE)
    logger.info("Consumer has no dedup logic — it processes every message.")
    logger.info("Expected: %d arrive, all %d processed (1 is a duplicate).",
                BATCH_SIZE + 1, BATCH_SIZE + 1)
    logger.info("═" * 62)

    r1 = run_phase(
        group_suffix="phase1",
        consumer_fn=run_naive_consumer,
        producer_send_fn=send_batch_no_ids,
        run_id="no-ids",
        simulate_retry_on=2,
    )

    logger.info("")
    logger.info("Phase 1 result ▶  %d received, %d processed  ← duplicate acted on",
                r1.get("total", 0), r1.get("processed", 0))

    # ── Phase 2: UUID message IDs, dedup consumer ──────────────────────────────
    logger.info("")
    logger.info("═" * 62)
    logger.info("PHASE 2 — UUID message IDs  (exactly-once / dedup consumer)")
    logger.info("Same retry simulation. Producer re-sends same UUID on retry.")
    logger.info("Consumer tracks seen IDs and skips anything already processed.")
    logger.info("Expected: %d arrive, %d processed, 1 skipped.", BATCH_SIZE + 1, BATCH_SIZE)
    logger.info("═" * 62)

    r2 = run_phase(
        group_suffix="phase2",
        consumer_fn=run_dedup_consumer,
        producer_send_fn=send_batch_with_ids,
        run_id="with-ids",
        simulate_retry_on=2,
    )

    logger.info("")
    logger.info("Phase 2 result ▶  %d received, %d processed, %d skipped  ← exactly-once",
                r2.get("total", 0), r2.get("processed", 0), r2.get("skipped", 0))

    # ── Summary ────────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("═" * 62)
    logger.info("SUMMARY")
    logger.info("  No IDs  / naive consumer  → %d received  %d processed  ← problem",
                r1.get("total", 0), r1.get("processed", 0))
    logger.info("  UUID ID / dedup consumer  → %d received  %d processed  %d skipped"
                "  ← solved",
                r2.get("total", 0), r2.get("processed", 0), r2.get("skipped", 0))
    logger.info("")
    logger.info("Producer change: add  message_id=str(uuid.uuid4())  to every record.")
    logger.info("On retry:        re-use the same message_id (don't generate a new one).")
    logger.info("Consumer change: maintain a seen_ids set; skip if message_id is in it.")
    logger.info("")
    logger.info("In production the seen_ids set is backed by Redis, a DB unique index,")
    logger.info("or an idempotency table — so it survives consumer restarts.")
    logger.info("═" * 62)
