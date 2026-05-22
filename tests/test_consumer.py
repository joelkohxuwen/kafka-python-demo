from unittest.mock import MagicMock, patch

from consumer import create_consumer, consume


@patch("consumer.KafkaConsumer")
def test_create_consumer_uses_correct_topic(mock_klass):
    create_consumer(topic="my-topic", broker="broker:9092", group_id="grp")
    mock_klass.assert_called_once()
    args, kwargs = mock_klass.call_args
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


def test_consume_processes_all_messages():
    msg1 = MagicMock(partition=0, offset=0, value={"index": 0})
    msg2 = MagicMock(partition=0, offset=1, value={"index": 1})
    consumer = [msg1, msg2]
    # consume() should iterate without raising
    consume(consumer)
