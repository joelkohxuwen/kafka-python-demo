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
def test_create_consumer_passes_listener_when_on_assign_given(mock_klass):
    callback = MagicMock()
    create_consumer(topic="t", broker="b:9092", group_id="g", on_assign=callback)
    instance = mock_klass.return_value
    _, kwargs = instance.subscribe.call_args
    # A SeekListener wrapping the callback should be passed as listener=
    assert kwargs["listener"] is not None


@patch("consumer.KafkaConsumer")
def test_create_consumer_no_listener_when_no_on_assign(mock_klass):
    create_consumer(topic="t", broker="b:9092", group_id="g")
    instance = mock_klass.return_value
    _, kwargs = instance.subscribe.call_args
    assert kwargs["listener"] is None


# ---------------------------------------------------------------------------
# on_assign seek behaviour
# ---------------------------------------------------------------------------

def test_on_assign_from_beginning_seeks_to_start():
    """SeekListener.on_partitions_assigned calls seek_to_beginning."""
    from consumer import SeekListener
    partitions = [TopicPartition("demo-topic", 0), TopicPartition("demo-topic", 1)]
    consumer = MagicMock()

    def on_assign(c, parts):
        c.seek_to_beginning(*parts)

    listener = SeekListener(consumer, on_assign)
    listener.on_partitions_assigned(partitions)
    consumer.seek_to_beginning.assert_called_once_with(*partitions)


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
