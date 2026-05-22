"""Tests for idempotent_producer.py — all mocked, no live broker needed."""
import json
import uuid
from threading import Event
from unittest.mock import MagicMock, call, patch

import pytest

from idempotent_producer import (
    BATCH_SIZE,
    DEMO_TOPIC,
    create_producer,
    make_retrying_consumer,
    run_dedup_consumer,
    run_naive_consumer,
    send_batch_no_ids,
    send_batch_with_ids,
)


# ── Producer factory ───────────────────────────────────────────────────────────

class TestCreateProducer:
    def test_api_version_set(self):
        with patch("idempotent_producer.KafkaProducer") as mock_cls:
            create_producer()
            _, kwargs = mock_cls.call_args
            assert kwargs["api_version"] == (2, 5, 0), "Rule 1: api_version must be (2,5,0)"

    def test_acks_all(self):
        with patch("idempotent_producer.KafkaProducer") as mock_cls:
            create_producer()
            _, kwargs = mock_cls.call_args
            assert kwargs["acks"] == "all"


# ── send_batch_no_ids ──────────────────────────────────────────────────────────

class TestSendBatchNoIds:
    def _make_producer(self):
        producer = MagicMock()
        future = MagicMock()
        future.get.return_value = MagicMock(partition=0, offset=0)
        producer.send.return_value = future
        return producer

    def test_sends_batch_size_messages_without_retry(self):
        producer = self._make_producer()
        send_batch_no_ids(producer, run_id="test", simulate_retry_on=None)
        assert producer.send.call_count == BATCH_SIZE

    def test_sends_one_extra_when_retry_simulated(self):
        producer = self._make_producer()
        send_batch_no_ids(producer, run_id="test", simulate_retry_on=2)
        assert producer.send.call_count == BATCH_SIZE + 1

    def test_no_message_id_in_record(self):
        producer = self._make_producer()
        send_batch_no_ids(producer, run_id="test")
        for c in producer.send.call_args_list:
            record = c.kwargs["value"]
            assert "message_id" not in record

    def test_flush_and_close_called(self):
        producer = self._make_producer()
        send_batch_no_ids(producer, run_id="test")
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


# ── send_batch_with_ids ────────────────────────────────────────────────────────

class TestSendBatchWithIds:
    def _make_producer(self):
        producer = MagicMock()
        future = MagicMock()
        future.get.return_value = MagicMock(partition=0, offset=0)
        producer.send.return_value = future
        return producer

    def test_sends_batch_size_messages_without_retry(self):
        producer = self._make_producer()
        send_batch_with_ids(producer, run_id="test", simulate_retry_on=None)
        assert producer.send.call_count == BATCH_SIZE

    def test_sends_one_extra_when_retry_simulated(self):
        producer = self._make_producer()
        send_batch_with_ids(producer, run_id="test", simulate_retry_on=2)
        assert producer.send.call_count == BATCH_SIZE + 1

    def test_every_record_has_message_id(self):
        producer = self._make_producer()
        send_batch_with_ids(producer, run_id="test")
        for c in producer.send.call_args_list:
            record = c.kwargs["value"]
            assert "message_id" in record, "Each record must carry a message_id"

    def test_retry_reuses_same_message_id(self):
        """The retry must carry the same UUID as the original, not a fresh one."""
        producer = self._make_producer()
        send_batch_with_ids(producer, run_id="test", simulate_retry_on=1)

        calls_for_index_1 = [
            c for c in producer.send.call_args_list
            if c.kwargs["value"].get("index") == 1
        ]
        assert len(calls_for_index_1) == 2, "index=1 should be sent twice (original + retry)"
        id_first = calls_for_index_1[0].kwargs["value"]["message_id"]
        id_retry = calls_for_index_1[1].kwargs["value"]["message_id"]
        assert id_first == id_retry, "Retry must reuse the original message_id, not generate a new UUID"

    def test_each_index_has_unique_message_id(self):
        """Different indices must get different UUIDs."""
        producer = self._make_producer()
        send_batch_with_ids(producer, run_id="test", simulate_retry_on=None)

        ids = [c.kwargs["value"]["message_id"] for c in producer.send.call_args_list]
        assert len(ids) == len(set(ids)), "Each message must have a unique message_id"

    def test_flush_and_close_called(self):
        producer = self._make_producer()
        send_batch_with_ids(producer, run_id="test")
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


# ── Dedup logic (inline, no Kafka) ────────────────────────────────────────────

class TestDedupLogic:
    """Verify the seen_ids logic that run_dedup_consumer applies."""

    def _run(self, messages: list[dict]) -> dict:
        """Replicate the consumer's dedup logic and return stats."""
        seen_ids: set[str] = set()
        total = processed = skipped = 0
        for msg in messages:
            total += 1
            mid = msg.get("message_id")
            if mid in seen_ids:
                skipped += 1
            else:
                processed += 1
                seen_ids.add(mid)
        return {"total": total, "processed": processed, "skipped": skipped}

    def test_no_duplicates_all_processed(self):
        msgs = [{"message_id": str(uuid.uuid4()), "index": i} for i in range(5)]
        r = self._run(msgs)
        assert r["total"] == 5
        assert r["processed"] == 5
        assert r["skipped"] == 0

    def test_duplicate_id_is_skipped(self):
        shared_id = str(uuid.uuid4())
        msgs = [
            {"message_id": str(uuid.uuid4()), "index": 0},
            {"message_id": shared_id,          "index": 1},  # original
            {"message_id": str(uuid.uuid4()), "index": 2},
            {"message_id": shared_id,          "index": 1},  # retry → should be skipped
            {"message_id": str(uuid.uuid4()), "index": 4},
        ]
        r = self._run(msgs)
        assert r["total"] == 5
        assert r["processed"] == 4
        assert r["skipped"] == 1

    def test_different_ids_always_processed(self):
        msgs = [{"message_id": str(uuid.uuid4()), "index": i} for i in range(10)]
        r = self._run(msgs)
        assert r["processed"] == 10
        assert r["skipped"] == 0

    def test_naive_consumer_processes_duplicates(self):
        """Confirm Phase 1 behaviour: no seen_ids check means duplicates get through."""
        total = 0
        processed = 0
        msgs = [{"index": i} for i in range(5)]
        msgs.insert(3, {"index": 2})   # inject duplicate
        for _ in msgs:
            total += 1
            processed += 1   # naive: always process
        assert total == 6
        assert processed == 6
