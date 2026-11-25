"""Avro wire-framed event contracts, 8 multimodal Kafka topics, and backward/forward evolution verification."""

from __future__ import annotations

import io
import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fastavro

MAGIC_BYTE = 0x00


class SchemaValidationError(ValueError):
    """Raised when an Avro frame fails magic-byte, schema-ID, or schema validation."""


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    key_domain: str
    schema_file: str
    schema_id: int
    retention_ms: int


TOPIC_SPECS: dict[str, TopicSpec] = {
    "multimodal.source.events": TopicSpec("multimodal.source.events", 12, "document_id", "source_asset_event.avsc", 1, 14 * 86400 * 1000),
    "multimodal.acl.updates": TopicSpec("multimodal.acl.updates", 6, "document_id", "source_asset_event.avsc", 1, 30 * 86400 * 1000),
    "multimodal.deletions.tombstones": TopicSpec("multimodal.deletions.tombstones", 6, "document_id", "deletion_tombstone_event.avsc", 2, 90 * 86400 * 1000),
    "multimodal.dead-letter": TopicSpec("multimodal.dead-letter", 6, "document_id", "dlq_event.avsc", 3, 30 * 86400 * 1000),
}


class AvroContractRegistry:
    def __init__(self, schemas_dir: Path) -> None:
        self.schemas_dir = schemas_dir
        self._raw_by_id: dict[int, dict[str, Any]] = {}
        self._parsed_by_id: dict[int, Any] = {}
        for spec in TOPIC_SPECS.values():
            if spec.schema_id not in self._parsed_by_id:
                raw = json.loads((schemas_dir / spec.schema_file).read_text(encoding="utf-8"))
                self._raw_by_id[spec.schema_id] = raw
                self._parsed_by_id[spec.schema_id] = fastavro.parse_schema(raw)

    def serialize(self, topic: str, record: dict[str, Any]) -> bytes:
        spec = TOPIC_SPECS[topic]
        parsed = self._parsed_by_id[spec.schema_id]
        buf = io.BytesIO()
        buf.write(struct.pack(">BI", MAGIC_BYTE, spec.schema_id))
        try:
            fastavro.schemaless_writer(buf, parsed, record)
        except Exception as exc:
            raise SchemaValidationError(f"Avro encode failed for {topic}: {exc}") from exc
        return buf.getvalue()

    def deserialize(self, payload: bytes, *, expected_topic: str | None = None) -> tuple[int, dict[str, Any]]:
        if len(payload) < 5 or payload[0] != MAGIC_BYTE:
            raise SchemaValidationError("Corrupt Avro wire header")
        schema_id = struct.unpack(">I", payload[1:5])[0]
        if schema_id not in self._parsed_by_id:
            raise SchemaValidationError(f"Unknown schema_id={schema_id}")
        if expected_topic is not None and schema_id != TOPIC_SPECS[expected_topic].schema_id:
            raise SchemaValidationError(f"Schema mismatch for {expected_topic}: got {schema_id}")
        buf = io.BytesIO(payload[5:])
        try:
            decoded = fastavro.schemaless_reader(buf, self._parsed_by_id[schema_id])
        except Exception as exc:
            raise SchemaValidationError(f"Avro decode failed: {exc}") from exc
        if not isinstance(decoded, dict):
            raise SchemaValidationError("Decoded payload is not a record")
        return schema_id, decoded

    def check_compatibility(
        self, writer_schema: dict[str, Any], reader_schema: dict[str, Any], sample: dict[str, Any]
    ) -> tuple[bool, str]:
        try:
            w = fastavro.parse_schema(writer_schema)
            r = fastavro.parse_schema(reader_schema)
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, w, sample)
            buf.seek(0)
            decoded = fastavro.schemaless_reader(buf, w, r)
            return (isinstance(decoded, dict), "compatible")
        except Exception as exc:
            return False, str(exc)
