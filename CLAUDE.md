# kafka-python-demo — project notes for Claude

## Windows Kafka rules (MUST follow every time)

`kafka-python-ng` has a Windows-specific bug: `ValueError: Invalid file descriptor: -1`.
It surfaces in several distinct situations, each with its own fix. All rules below were
discovered through actual failures in this project — do not skip any of them.

---

### Rule 1 — `api_version=(2, 5, 0)` on EVERY consumer and producer

Without it, the library auto-negotiates the broker API version by opening extra internal
sockets that fail on Windows.

```python
# CORRECT
KafkaConsumer("my-topic", bootstrap_servers=..., api_version=(2, 5, 0), ...)
KafkaProducer(bootstrap_servers=..., api_version=(2, 5, 0), ...)

# WRONG — omitting api_version causes fd=-1
KafkaProducer(bootstrap_servers=...)
```

This applies to **every** script — producer.py, consumer.py, dlq_consumer.py,
demo_dlq.py, and any new file added in future.

---

### Rule 2 — Pass topic to `KafkaConsumer` constructor, never via `subscribe()`

A separate `consumer.subscribe([topic])` call triggers extra socket operations that
cause fd=-1 even when `api_version` is set.

```python
# CORRECT
KafkaConsumer("my-topic", bootstrap_servers=..., api_version=(2, 5, 0))

# WRONG — two-step subscribe causes fd=-1 on Windows
consumer = KafkaConsumer(bootstrap_servers=...)
consumer.subscribe(["my-topic"])
```

---

### Rule 3 — Never run multiple Python processes that each hold Kafka connections

Even with `api_version` set, having several processes each open their own Kafka
connections simultaneously causes fd=-1. Consolidate into **one process using threads**.

```
# WRONG — 3 processes, each with connections → fd=-1
Terminal 1: python consumer.py --dlq     # KafkaConsumer + KafkaProducer
Terminal 2: python dlq_consumer.py       # KafkaConsumer
Terminal 3: python producer.py           # KafkaProducer

# CORRECT — 2 processes, multi-connection script uses threads internally
Terminal 1: python demo_dlq.py           # threads share one process
Terminal 2: python producer.py
```

---

### Rule 4 — Stagger thread startup when a single process holds multiple connections

Starting two threads simultaneously causes both to race for broker connections at the
same moment, triggering fd=-1. Add `time.sleep(3)` between thread starts.

```python
# CORRECT
main_thread.start()
time.sleep(3)   # let the first connection stabilise before opening another
dlq_thread.start()

# WRONG — simultaneous start causes connection race → fd=-1
main_thread.start()
dlq_thread.start()
```

---

### Rule 5 — Create secondary Kafka connections lazily, not at startup

If a script needs a KafkaProducer only conditionally (e.g. a DLQ producer that fires
on failures), do NOT create it at startup alongside the consumer. Create it on first use.
Opening two connections at once during initialisation triggers fd=-1.

```python
# CORRECT — lazy creation
dlq_producer = None
for message in consumer:
    try:
        process(message)
    except Exception:
        if dlq_producer is None:
            dlq_producer = KafkaProducer(..., api_version=(2, 5, 0))
        dlq_producer.send(DLQ_TOPIC, ...)

# WRONG — both connections open at startup
consumer = KafkaConsumer(...)
dlq_producer = KafkaProducer(...)   # second connection too early → fd=-1
```

---

### Rule 6 — Wrap long-running threads with a retry loop for fd=-1

Even with all the above rules, fd=-1 can still occur transiently on startup.
Threads must catch it and retry rather than dying permanently.

```python
def with_retry(fn, name, delay=2.0):
    while True:
        try:
            fn()
            break
        except ValueError as exc:
            if "Invalid file descriptor" in str(exc):
                logger.warning("%s: connection reset — retrying in %.0fs...", name, delay)
                time.sleep(delay)
            else:
                raise

threading.Thread(target=with_retry, args=(run_consumer, "consumer")).start()
```

---

### Rule 7 — Set short session timeouts so the broker clears stale sessions quickly

After a hard restart (Ctrl+C), the broker holds the old consumer group session until
`session_timeout_ms` expires. With the default (10s), restarting within that window
triggers a rebalance on a dead connection → fd=-1 infinite retry loop.
Set `session_timeout_ms=6000` and `heartbeat_interval_ms=2000` on every consumer.

```python
KafkaConsumer(
    "my-topic",
    bootstrap_servers=...,
    api_version=(2, 5, 0),
    session_timeout_ms=6000,    # broker clears stale sessions in 6s
    heartbeat_interval_ms=2000, # must stay < session_timeout_ms / 3
)
```

---

### Rule 8 — Use exponential backoff with a retry cap in `with_retry`

An infinite retry loop (`while True`) will spin forever if the broker is genuinely
unreachable (e.g. stale session, Docker restart). Use exponential backoff and a
`max_retries` limit so the thread eventually gives up instead of hammering the broker.

```python
delay = 2.0
for attempt in range(1, max_retries + 1):
    try:
        fn(); return
    except ValueError as exc:
        if "Invalid file descriptor" in str(exc):
            time.sleep(delay)
            delay = min(delay * 2, 30.0)  # cap at 30s
        else:
            raise
logger.error("Giving up after %d attempts.", max_retries)
```

---

### Rule 9 — Never use `thread.join()` without a timeout on Windows

`thread.join()` with no timeout blocks the Python signal handler, making Ctrl+C
unresponsive. Always join inside a polling loop with a short timeout instead.

```python
# CORRECT — Ctrl+C works
while thread.is_alive():
    thread.join(timeout=0.5)

# WRONG — Ctrl+C is swallowed on Windows
thread.join()
```

---

### Checklist before adding any new consumer or producer

- [ ] `api_version=(2, 5, 0)` present?
- [ ] Topic passed to `KafkaConsumer` constructor (not via `subscribe()`)?
- [ ] Is this a new process, or does it share a process with other connections?
      If new process: merge into existing process using threads instead.
- [ ] If multiple threads: is there a `time.sleep(3)` between `.start()` calls?
- [ ] Is any KafkaProducer created conditionally? If so, is it lazy (created on first use)?
- [ ] Is the thread wrapped in `with_retry`?

---

## Dependencies

- `kafka-python-ng==2.2.3` — drop-in replacement for `kafka-python`, fixes Python 3.12
  compatibility (`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`).
  Do NOT revert to `kafka-python`.

## Project structure

| File | Purpose |
|---|---|
| `config.py` | Broker address, topic names, group ID |
| `schema_registry.py` | Thin REST client for Confluent Schema Registry |
| `docker-compose.yml` | Kafka + Schema Registry infrastructure (one command) |
| `demos/producer.py` | Sends messages; `--key` for partition keys, `--v2` for schema v2 |
| `demos/consumer.py` | Reads messages; `--group`, `--from-beginning`, `--seek-to`, `--dlq` |
| `demos/dlq_consumer.py` | Standalone DLQ reader (run alone, not alongside consumer.py) |
| `demos/demo_dlq.py` | Combined DLQ demo — runs main consumer + DLQ inspector in one process |
| `demos/show_partitions.py` | Utility — prints which partition each key hashes to |
| `demos/avro_producer.py` | Avro producer with Confluent wire format + Schema Registry |
| `demos/avro_consumer.py` | Avro consumer — reads schema ID from each message, fetches schema |
| `demos/idempotent_producer.py` | Idempotent producer demo — shows at-least-once vs exactly-once |

## Running locally

Requires a Kafka broker at `localhost:9092`. Spin one up with Docker:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0
```

`demo-topic` needs 2 partitions (run once after broker starts):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic demo-topic --partitions 2 --replication-factor 1
```

Tests use mocks and do not need a live broker:

```bash
python -m pytest tests/ -v
```
