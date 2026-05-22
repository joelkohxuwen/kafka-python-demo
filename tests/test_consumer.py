from unittest.mock import MagicMock, patch
from kafka.structs import TopicPartition

from consumer import create_consumer, consume, apply_seek


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
    consumer.seek_to_beginning.assert_called_once_with(p0, p1)


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
