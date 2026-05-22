"""Thin client for the Confluent Schema Registry REST API.

Implements only what this demo needs:
  - register a schema under a subject
  - fetch a schema by ID
  - check whether a schema is compatible with the current version
"""
import json
import requests

SCHEMA_REGISTRY_URL = "http://localhost:8081"


class SchemaRegistryError(Exception):
    pass


def register_schema(subject: str, schema: dict) -> int:
    """Register schema under subject. Returns the schema ID.

    If the exact schema already exists, the registry returns the existing ID
    (idempotent). Raises SchemaRegistryError if the schema is incompatible.
    """
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(schema)}
    resp = requests.post(url, json=payload)

    if resp.status_code == 409:
        raise SchemaRegistryError(
            f"Incompatible schema for subject '{subject}': {resp.json().get('message')}"
        )
    resp.raise_for_status()
    return resp.json()["id"]


def get_schema_by_id(schema_id: int) -> dict:
    """Fetch a schema definition by its integer ID."""
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}")
    resp.raise_for_status()
    return json.loads(resp.json()["schema"])


def get_latest_version(subject: str) -> tuple[int, dict]:
    """Return (schema_id, schema_dict) for the latest version of a subject."""
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest")
    resp.raise_for_status()
    data = resp.json()
    return data["id"], json.loads(data["schema"])


def check_compatibility(subject: str, schema: dict) -> bool:
    """Return True if schema is compatible with the latest registered version."""
    url = f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest"
    resp = requests.post(url, json={"schema": json.dumps(schema)})
    if resp.status_code == 404:
        return True  # no existing version — always compatible
    resp.raise_for_status()
    return resp.json().get("is_compatible", False)


def list_subjects() -> list[str]:
    """Return all registered subjects."""
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
    resp.raise_for_status()
    return resp.json()
