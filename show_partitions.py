"""Utility: show which partition each key maps to (useful for demos)."""
from kafka.partitioner.default import murmur2

NUM_PARTITIONS = 2

SAMPLE_KEYS = [
    "user-123", "user-456", "user-789",
    "order-A",  "order-B",  "order-C",
    "alice",    "bob",      "charlie",
    "session-1","session-2","session-3",
]

print(f"{'Key':<15} Partition  (topic has {NUM_PARTITIONS} partitions)")
print("-" * 35)
for key in SAMPLE_KEYS:
    h = murmur2(key.encode("utf-8"))
    partition = (h & 0x7FFFFFFF) % NUM_PARTITIONS
    print(f"{key:<15} {partition}")
