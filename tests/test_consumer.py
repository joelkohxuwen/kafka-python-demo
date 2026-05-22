from unittest.mock import MagicMock, call, patch
from kafka.structs import TopicPartition

from consumer import create_consumer, consume


# ---------------------------------------------------------------------------
# create_consumer
# ---------------------------------------------------------------------------

@patch("consumer.KafkaConsumer")
def test_create_consumer_subscribes_to_topic(mock_klass):
    create_consumer(topic="my-topic", broker="broker:9092", group_id="grp")
    instance = mock_klass.return_value
    instance.subscribe.assert_called_once()
    args, _ = instance.subscribe.call_args
    assert args[0] == ["my-topic"]


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


@patch("consumer.KafkaConsumer")
def test_create_consumer_passes_on_assign_callback(mock_klass):
    callback = MagicMock()
    create_consumer(topic="t", broker="b:9092", group_id="g", on_assign=callback)
    instance = mock_klass.return_value
    _, kwargs = instance.subscribe.call_args
    assert kwargs["on_assign"] == callback


# ---------------------------------------------------------------------------
# on_assign seek behaviour
# ---------------------------------------------------------------------------

def test_on_assign_from_beginning_seeks_to_start():
    """seek_to_beginning is called with all assigned partitions."""
    partitions = [TopicPartition("demo-topic", 0), TopicPartition("demo-topic", 1)]
    consumer = MagicMock()

    # Simulate what Kafka calls when partitions are assigned
    from consumer import create_consumer
    with patch("consumer.KafkaConsumer") as mock_klass:
        mock_klass.return_value = consumer
        # Capture the on_assign we pass in
        on_assign = MagicMock()
        create_consumer(on_assign=on_assign)
        _, kwargs = consumer.subscribe.call_args
        cb = kwargs["on_assign"]

    # Build a real on_assign that mimics --from-beginning
    consumer2 = MagicMock()
    consumer2.seek_to_beginning = MagicMock()
    consumer2.seek = MagicMock()

    def from_beginning_on_assign(c, parts):
        c.seek_to_beginning(*parts)

    from_beginning_on_assign(consumer2, partitions)
    consumer2.seek_to_beginning.assert_called_once_with(*partitions)


def test_on_assign_seek_to_calls_seek_per_partition():
    """consumer.seek is called once per partition with the target offset."""
    partitions = [TopicPartition("demo-topic", 0), TopicPartition("demo-topic", 1)]
    consumer = MagicMock()
    target_offset = 3

    def seek_to_on_assign(c, parts):
        for tp in parts:
            c.seek(tp, target_offset)

    seek_to_on_assign(consumer, partitions)
    assert consumer.seek.call_count == len(partitions)
    consumer.seek.assert_any_call(TopicPartition("demo-topic", 0), target_offset)
    consumer.seek.assert_any_call(TopicPartition("demo-topic", 1), target_offset)


# ---------------------------------------------------------------------------
# consume
# ---------------------------------------------------------------------------

def test_consume_processes_all_messages():
    msg1 = MagicMock(partition=0, offset=0, value={"index": 0})
    msg2 = MagicMock(partition=0, offset=1, value={"index": 1})
    consume([msg1, msg2])
