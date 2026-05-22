# kafka-python-demo

A hands-on Kafka learning project in Python covering five core concepts: partition keys, consumer offset management, dead letter queues, schema evolution with Schema Registry, and idempotent producers.

Built with [`kafka-python-ng`](https://github.com/pdeantoni/kafka-python-ng) (a Python 3.12-compatible fork of kafka-python) and [`fastavro`](https://fastavro.readthedocs.io/) for Avro serialisation.

> **Windows users:** `kafka-python-ng` has a known `ValueError: Invalid file descriptor: -1` bug on Windows. All scripts in this repo include the necessary workarounds. See [`CLAUDE.md`](CLAUDE.md) for the full list of rules.

---

## Prerequisites

- Python 3.12+
- Docker Desktop

---

## Setup

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Start Kafka with dual listeners** (required for Schema Registry to reach Kafka inside Docker)
```bash
docker network create kafka-net

docker run -d --name kafka \
  --network kafka-net \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT_HOST://0.0.0.0:9092,PLAINTEXT_INT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://localhost:9092,PLAINTEXT_INT://kafka:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT_INT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:3.7.0
```

**3. Create the demo topic with 2 partitions**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic demo-topic --partitions 2 --replication-factor 1
```

**4. Start Schema Registry** (only needed for Concept 4)
```bash
docker run -d --name schema-registry \
  --network kafka-net \
  -p 8081:8081 \
  -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:29092 \
  confluentinc/cp-schema-registry:7.6.0
```

---

## Concepts

### 1 — Partition keys & consumer groups

Kafka uses a hash of the message key to decide which partition it lands on. Messages with the same key always go to the same partition, guaranteeing ordering per key. A consumer group load-balances across partitions — each partition is owned by exactly one consumer in the group.

**Run in three terminals:**
```bash
# Terminal 1
python consumer.py --group group-a

# Terminal 2
python consumer.py --group group-a

# Terminal 3 — send messages pinned to two keys
python producer.py --key user-123
python producer.py --key order-A
```

Watch each key always land on the same partition, and the two consumers split the partitions between them. Run `show_partitions.py` to see the murmur2 hash mapping upfront:
```bash
python show_partitions.py
```

---

### 2 — Consumer offset management

Kafka retains messages on disk. Consumers track their position (offset) per partition, so they can replay old messages or skip ahead.

**Replay from the beginning:**
```bash
python consumer.py --from-beginning
```

**Seek to a specific offset:**
```bash
python consumer.py --seek-to 5
```

Two consumer groups reading the same topic maintain completely independent offsets — neither affects the other.

---

### 3 — Dead Letter Queue (DLQ)

When a consumer can't process a message (bad data, downstream failure), sending it to a DLQ keeps the pipeline moving without losing the message. It can be replayed, inspected, or routed to an alert.

The demo simulates a failure on every odd-indexed message. Failed messages are forwarded to `demo-topic-dlq` with metadata (original topic, partition, offset, error).

```bash
python demo_dlq.py
```

Both the main consumer and DLQ inspector run in a single process (two threads) to avoid the Windows multi-process fd=-1 issue.

In a second terminal, produce some messages to trigger failures:
```bash
python producer.py
```

---

### 4 — Schema evolution & Schema Registry

As your data model evolves, old consumers must still be able to read new messages and vice versa. Confluent Schema Registry enforces compatibility rules at registration time — before any incompatible message reaches the topic.

This demo uses Avro serialisation with the Confluent wire format:
```
[0x00] magic byte | [4 bytes] schema ID | [remaining] Avro binary
```

Every message carries its schema ID, so the consumer always knows which schema to use — no code changes needed when a new schema version is deployed.

**Run in two terminals:**
```bash
# Terminal 1 — start the consumer
python avro_consumer.py

# Terminal 2 — register v1 and send 5 messages
python avro_producer.py --schema schemas/demo_message_v1.avsc

# Terminal 2 — upgrade to v2 (additive fields with defaults — accepted)
python avro_producer.py --schema schemas/demo_message_v2.avsc

# Terminal 2 — try a breaking change (int → string on index — rejected)
python avro_producer.py --schema schemas/demo_message_v2_breaking.avsc
```

The breaking schema is rejected by the registry before a single message is sent. The consumer handles v1 and v2 messages side-by-side with no code changes.

**Schema files:**

| File | Description |
|---|---|
| `schemas/demo_message_v1.avsc` | `index` (int) + `msg` (string) |
| `schemas/demo_message_v2.avsc` | Adds `timestamp` (nullable) + `schema_version` (int, default 1) |
| `schemas/demo_message_v2_breaking.avsc` | Changes `index` int → string — intentionally incompatible |

---

### 5 — Idempotent producer

In an at-least-once system, a producer retries when it doesn't receive an ack. If the original message was written but the ack was lost, the retry produces a duplicate. For payments, inventory, or any operation that must not be applied twice, this is a problem.

The solution: stamp every message with a unique `message_id` (UUID) generated before the first send attempt. On retry, re-use the same UUID. The consumer tracks a set of seen IDs and skips any message whose ID has already been processed.

```bash
python idempotent_producer.py
```

The script runs two phases automatically:
- **Phase 1** — no message IDs; a simulated retry produces a duplicate that the naive consumer acts on twice
- **Phase 2** — UUID message IDs; the same retry arrives with the same ID and is silently skipped

```
SUMMARY
  No IDs  / naive consumer  → 6 received  6 processed  ← problem
  UUID ID / dedup consumer  → 6 received  5 processed  1 skipped  ← solved
```

> In production the `seen_ids` set is backed by Redis, a database unique constraint, or an idempotency table so it survives consumer restarts.

---

## File structure

| File | Purpose |
|---|---|
| `producer.py` | Sends messages; `--key` for partition keys, `--v2` for schema v2 |
| `consumer.py` | Reads messages; `--group`, `--from-beginning`, `--seek-to`, `--dlq` |
| `demo_dlq.py` | DLQ demo — main consumer + DLQ inspector in one process (two threads) |
| `dlq_consumer.py` | Standalone DLQ reader |
| `avro_producer.py` | Avro producer with Schema Registry integration |
| `avro_consumer.py` | Avro consumer — reads schema ID from each message, fetches schema |
| `schema_registry.py` | Thin REST client for Confluent Schema Registry |
| `idempotent_producer.py` | Idempotent producer demo — at-least-once vs exactly-once |
| `show_partitions.py` | Utility — prints which partition each key hashes to |
| `config.py` | Broker address, topic names, consumer group ID |

---

## Configuration

Edit [`config.py`](config.py) to change broker, topic, or group settings.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `TOPIC` | `demo-topic` | Main topic name |
| `DLQ_TOPIC` | `demo-topic-dlq` | Dead letter queue topic |
| `GROUP_ID` | `demo-group` | Base consumer group ID |

---

## Tests

All tests use mocks — no live broker required.

```bash
python -m pytest tests/ -v
```
