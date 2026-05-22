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
    producer.send.assert_called_once_with("demo-topic", value={"key": "value"})


def test_send_message_waits_for_ack():
    producer = make_mock_producer()
    send_message(producer, "demo-topic", {"key": "value"})
    producer.send.return_value.get.assert_called_once_with(timeout=10)
