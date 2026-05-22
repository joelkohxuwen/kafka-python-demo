# kafka-python-demo — project notes for Claude

## Windows Kafka fix (MUST follow every time)

`kafka-python-ng` has a Windows-specific bug: `ValueError: Invalid file descriptor: -1`.
It surfaces whenever a new `KafkaConsumer` or `KafkaProducer` is instantiated without
an explicit API version, because the automatic broker version negotiation opens extra
internal sockets that fail on Windows.

### Rules — no exceptions

1. **Every `KafkaConsumer(...)` call must include `api_version=(2, 5, 0)`.**
2. **Every `KafkaProducer(...)` call must include `api_version=(2, 5, 0)`.**
3. **Pass the topic to the `KafkaConsumer` constructor directly** — do NOT call
   `consumer.subscribe([topic])` in a separate step. The two-step path triggers
   additional socket operations that expose the bug even with `api_version` set.

### Correct pattern

```python
# Consumer
KafkaConsumer(
    "my-topic",                          # topic in constructor, not subscribe()
    bootstrap_servers="localhost:9092",
    api_version=(2, 5, 0),              # REQUIRED on Windows
    ...
)

# Producer
KafkaProducer(
    bootstrap_servers="localhost:9092",
    api_version=(2, 5, 0),              # REQUIRED on Windows
    ...
)
```

### Wrong pattern (will cause fd=-1 on Windows)

```python
consumer = KafkaConsumer(bootstrap_servers=..., ...)  # no topic
consumer.subscribe(["my-topic"])                       # separate subscribe — DON'T DO THIS

KafkaProducer(bootstrap_servers=...)                   # missing api_version — DON'T DO THIS
```

### Checklist when adding new consumers or producers

- [ ] `api_version=(2, 5, 0)` present?
- [ ] Topic passed to `KafkaConsumer` constructor (not via `subscribe()`)?
- [ ] If multiple consumers/producers are needed simultaneously, are they in the **same process** using threads rather than separate processes? Multiple processes each holding Kafka connections causes fd=-1 even with `api_version` set.

## Dependencies

- `kafka-python-ng==2.2.3` — drop-in replacement for `kafka-python`, fixes Python 3.12
  compatibility (`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`).
  Do NOT revert to `kafka-python`.

## Running locally

Requires a Kafka broker at `localhost:9092`. Spin one up with Docker:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0
```

Tests use mocks and do not need a live broker:

```bash
python -m pytest tests/ -v
```
