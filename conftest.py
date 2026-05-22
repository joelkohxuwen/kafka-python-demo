"""Make project root and demos/ available on sys.path for all pytest sessions."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))          # for config, schema_registry
sys.path.insert(0, str(ROOT / "demos"))  # for producer, consumer, idempotent_producer
