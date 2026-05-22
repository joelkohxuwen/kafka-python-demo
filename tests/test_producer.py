from unittest.mock import MagicMock, patch
import pytest

from producer import send_message


def make_mock_producer(offset=42, partition=0, topic="demo-topic"):
    future = MagicMock()
    meta = MagicMock()
    meta.topic = topic
    meta.partition = partition
    meta.offset = offset
    future.get.return_value = meta

    producer = MagicMock()
    producer.send.return_value = future
    return producer


def test_send_message_calls_send():
    producer = make_mock_producer()
    send_message(producer, "demo-topic", {"key": "value"})
    producer.send.assert_called_once_with("demo-topic", value={"key": "value"}, key=None)


def test_send_message_waits_for_ack():
    producer = make_mock_producer()
    send_message(producer, "demo-topic", {"key": "value"})
    producer.send.return_value.get.assert_called_once_with(timeout=10)


def test_send_message_passes_key():
    producer = make_mock_producer()
    send_message(producer, "demo-topic", {"key": "value"}, key="user-123")
    producer.send.assert_called_once_with("demo-topic", value={"key": "value"}, key="user-123")


def test_send_message_no_key_defaults_to_none():
    producer = make_mock_producer()
    send_message(producer, "demo-topic", {"key": "value"})
    _, kwargs = producer.send.call_args
    assert kwargs["key"] is None
