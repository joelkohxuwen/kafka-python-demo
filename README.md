# kafka-python-demo

A minimal Kafka producer/consumer example in Python using [kafka-python](https://github.com/dpkp/kafka-python).

## Requirements

- Python 3.9+
- A running Kafka broker at `localhost:9092` (see [Quick Start](#quick-start))

## Setup

```bash
pip install -r requirements.txt
```

## Usage

**Producer** — sends 5 messages to `demo-topic`:

```bash
python producer.py
```

**Consumer** — reads messages from `demo-topic` until interrupted:

```bash
python consumer.py
```

## Configuration

Edit [`config.py`](config.py) to change the broker address, topic, or consumer group.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `TOPIC` | `demo-topic` | Topic name |
| `GROUP_ID` | `demo-group` | Consumer group ID |

## Tests

```bash
pytest tests/
```

## Quick Start

Spin up a local Kafka broker with Docker:

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  apache/kafka:3.7.0
```
