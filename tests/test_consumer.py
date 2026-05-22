from unittest.mock import MagicMock, patch
from kafka.structs import TopicPartition

import pytest
from consumer import create_consumer, consume, apply_seek, process_message, process_message_strict, consume_with_dlq


# ---------------------------------------------------------------------------
# create_consumer
# ---------------------------------------------------------------------------

@patch("consumer.KafkaConsumer")
def test_create_consumer_uses_correct_topic(mock_klass):
    create_consumer(topic="my-topic", broker="broker:9092", group_id="grp")
    args, _ = mock_klass.call_args
    assert args[0] == "my-topic"


@patch("consumer.KafkaConsumer")
def test_create_consumer_sets_group_id(mock_klass):
    create_consumer(topic="t", broker="b:9092", group_id="my-group")
    _, kwargs = mock_klass.call_args
    assert kwargs["group_id"] == "my-group"


@patch("consumer.KafkaConsumer")
def test_create_consumer_resets_to_earliest(mock_klass):
    create_consumer(topic="t", broker="b:9092", group_id="g")
    _, kwargs = mock_klass.call_args
    assert kwargs["auto_offset_reset"] == "earliest"


# ---------------------------------------------------------------------------
# apply_seek
# ---------------------------------------------------------------------------

def _make_consumer_with_partitions(*partitions):
    """Return a mock consumer that reports the given assigned partitions."""
    consumer = MagicMock()
    consumer.poll.return_value = {}
    consumer.assignment.return_value = set(partitions)
    return consumer


def test_apply_seek_no_op_when_neither_flag_set():
    consumer = MagicMock()
    apply_seek(consumer, from_beginning=False, seek_to=None)
    consumer.poll.assert_not_called()


def test_apply_seek_from_beginning_calls_seek_to_beginning():
    p0 = TopicPartition("demo-topic", 0)
    p1 = TopicPartition("demo-topic", 1)
    consumer = _make_consumer_with_partitions(p0, p1)

    apply_seek(consumer, from_beginning=True, seek_to=None)

    consumer.poll.assert_called_once()
    consumer.seek_to_beginning.assert_called_once()
    # assignment() returns a set so order is non-deterministic — check as a set
    called_with = set(consumer.seek_to_beginning.call_args[0])
    assert called_with == {p0, p1}


def test_apply_seek_to_calls_seek_per_partition():
    p0 = TopicPartition("demo-topic", 0)
    p1 = TopicPartition("demo-topic", 1)
    consumer = _make_consumer_with_partitions(p0, p1)

    apply_seek(consumer, from_beginning=False, seek_to=3)

    consumer.poll.assert_called_once()
    assert consumer.seek.call_count == 2
    consumer.seek.assert_any_call(p0, 3)
    consumer.seek.assert_any_call(p1, 3)


def test_apply_seek_warns_when_no_partitions_assigned():
    consumer = MagicMock()
    consumer.poll.return_value = {}
    consumer.assignment.return_value = set()  # nothing assigned yet

    # Should not raise, just log a warning
    apply_seek(consumer, from_beginning=True, seek_to=None)
    consumer.seek_to_beginning.assert_not_called()


# ---------------------------------------------------------------------------
# consume
# ---------------------------------------------------------------------------

def test_consume_processes_all_messages():
    msg1 = MagicMock(partition=0, offset=0, value={"index": 0})
    msg2 = MagicMock(partition=0, offset=1, value={"index": 1})
    consume([msg1, msg2])


# ---------------------------------------------------------------------------
# process_message
# ---------------------------------------------------------------------------

def test_process_message_succeeds_for_even_index():
    process_message({"index": 0, "msg": "hello-0"})
    process_message({"index": 2, "msg": "hello-2"})


def test_process_message_raises_for_odd_index():
    with pytest.raises(ValueError, match="Simulated failure for index 1"):
        process_message({"index": 1, "msg": "hello-1"})


def test_process_message_handles_v1_schema():
    """v1 messages (no timestamp/schema_version) must not raise."""
    process_message({"index": 0, "msg": "hello-0"})


def test_process_message_handles_v2_schema():
    """v2 messages with extra fields must be processed correctly."""
    process_message({
        "index": 0,
        "msg": "hello-0",
        "timestamp": "2026-05-22T00:00:00+00:00",
        "schema_version": 2,
    })


def test_process_message_v2_producer_v1_consumer_compatible():
    """A v1 consumer receiving a v2 message must not crash (forward compat)."""
    v2_message = {
        "index": 2,
        "msg": "hello-2",
        "timestamp": "2026-05-22T00:00:00+00:00",
        "schema_version": 2,
        "extra_future_field": "ignored",  # unknown fields are safely ignored
    }
    process_message(v2_message)  # should not raise


# ---------------------------------------------------------------------------
# process_message_strict — demonstrates schema evolution failure
# ---------------------------------------------------------------------------

def test_strict_works_on_v2_message():
    """Strict consumer handles v2 messages correctly — they have all expected fields."""
    process_message_strict({
        "index": 0,
        "msg": "hello-0",
        "timestamp": "2026-05-22T00:00:00+00:00",
        "schema_version": 2,
    })


def test_strict_crashes_on_v1_message():
    """Strict consumer crashes on v1 messages — this is the failure schema evolution prevents."""
    with pytest.raises(KeyError):
        process_message_strict({"index": 0, "msg": "hello-0"})  # no timestamp → KeyError


# ---------------------------------------------------------------------------
# consume_with_dlq
# ---------------------------------------------------------------------------

def _make_message(index, partition=0, offset=0, topic="demo-topic"):
    msg = MagicMock()
    msg.value = {"index": index, "msg": f"hello-{index}"}
    msg.topic = topic
    msg.partition = partition
    msg.offset = offset
    return msg


def test_consume_with_dlq_routes_failures_to_dlq():
    good = _make_message(0)   # even → processed OK
    bad  = _make_message(1)   # odd  → routed to DLQ

    dlq_producer = MagicMock()
    consume_with_dlq([good, bad], dlq_producer, dlq_topic="test-dlq")

    dlq_producer.send.assert_called_once()
    _, kwargs = dlq_producer.send.call_args
    assert kwargs["value"]["original_offset"] == bad.offset
    assert "Simulated failure" in kwargs["value"]["error"]


def test_consume_with_dlq_does_not_send_good_messages_to_dlq():
    good1 = _make_message(0)
    good2 = _make_message(2)

    dlq_producer = MagicMock()
    consume_with_dlq([good1, good2], dlq_producer, dlq_topic="test-dlq")

    dlq_producer.send.assert_not_called()
