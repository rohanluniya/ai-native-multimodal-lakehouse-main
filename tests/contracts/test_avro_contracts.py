"""Avro contract tests validating wire framing, v1<->v2 evolution compatibility, and malformed rejection."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from multimodal_lakehouse.config import LakehouseConfig
from multimodal_lakehouse.contracts import AvroContractRegistry, SchemaValidationError


def test_avro_roundtrip_and_v1_v2_evolution() -> None:
    cfg = LakehouseConfig()
    sdir = cfg.project_root / "schemas" / "avro"
    reg = AvroContractRegistry(sdir)
    now = datetime(2026, 11, 24, 18, 0, tzinfo=UTC).isoformat()

    sample_v1 = {
        "event_id": "ev-1",
        "event_type": "asset_upserted",
        "event_time": now,
        "ingestion_time": now,
        "schema_version": "1.0.0",
        "document_id": "DOC-1",
        "version_num": 1,
        "source_connector": "http_api",
        "source_uri": "s3://raw/DOC-1/v1",
        "modality": "pdf",
        "mime_type": "application/pdf",
        "content_sha256": "a" * 64,
        "byte_size": 1024,
        "title": "Test PDF",
        "language_hint": "en",
        "security_tier": "public",
        "acl_allow_groups": ["public"],
        "acl_deny_groups": [],
        "payload_path": None,
        "metadata_json": "{}",
        "trace_id": "t1",
        "correlation_id": "c1",
    }
    wire = reg.serialize("multimodal.source.events", sample_v1)
    sid, decoded = reg.deserialize(wire, expected_topic="multimodal.source.events")
    assert sid == 1
    assert decoded["document_id"] == "DOC-1"

    v1 = json.loads((sdir / "source_asset_event.avsc").read_text(encoding="utf-8"))
    v2 = json.loads((sdir / "v2" / "source_asset_event.avsc").read_text(encoding="utf-8"))
    sample_v2 = {**sample_v1, "schema_version": "2.0.0", "retention_policy_days": 365, "data_owner_team": "ai_platform"}
    b_ok, b_msg = reg.check_compatibility(v1, v2, sample_v1)
    f_ok, f_msg = reg.check_compatibility(v2, v1, sample_v2)
    assert b_ok, b_msg
    assert f_ok, f_msg


def test_avro_rejects_corrupt_payloads() -> None:
    reg = AvroContractRegistry(LakehouseConfig().project_root / "schemas" / "avro")
    with pytest.raises(SchemaValidationError):
        reg.deserialize(b"corrupt-bytes", expected_topic="multimodal.source.events")
    with pytest.raises(SchemaValidationError):
        reg.serialize("multimodal.source.events", {"event_id": "incomplete"})
