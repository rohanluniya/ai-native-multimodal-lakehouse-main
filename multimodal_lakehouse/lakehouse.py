"""Apache Iceberg Format V2 Multimodal Lakehouse, SCD2 Document/ACL History, Tombstone Erasure & Data Quality Engine."""

from __future__ import annotations

import base64
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from multimodal_lakehouse.chunking import VersionedChunk, chunk_artifact
from multimodal_lakehouse.config import LakehouseConfig
from multimodal_lakehouse.contracts import AvroContractRegistry, SchemaValidationError
from multimodal_lakehouse.evaluation import run_evaluation_suite
from multimodal_lakehouse.observability import OpenLineageEmitter, StructuredLogger, get_metrics
from multimodal_lakehouse.parsing import MediaCorruptError, parse_multimodal_asset
from multimodal_lakehouse.retrieval import HybridRetrievalIndex
from multimodal_lakehouse.transport import EventBus


def _parse_dt(iso: str | None) -> datetime:
    if not iso:
        return datetime.now(UTC).replace(tzinfo=None)
    dt = datetime.fromisoformat(iso)
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def _bronze_assets_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "document_id", StringType(), required=True),
        NestedField(3, "version_num", IntegerType(), required=True),
        NestedField(4, "event_time", TimestampType(), required=True),
        NestedField(5, "ingestion_time", TimestampType(), required=True),
        NestedField(6, "source_connector", StringType(), required=True),
        NestedField(7, "source_uri", StringType(), required=True),
        NestedField(8, "modality", StringType(), required=True),
        NestedField(9, "mime_type", StringType(), required=True),
        NestedField(10, "content_sha256", StringType(), required=True),
        NestedField(11, "byte_size", LongType(), required=True),
        NestedField(12, "title", StringType(), required=True),
        NestedField(13, "security_tier", StringType(), required=True),
        NestedField(14, "acl_allow_csv", StringType(), required=True),
        NestedField(15, "acl_deny_csv", StringType(), required=True),
    )


def _bronze_quarantine_schema() -> Schema:
    return Schema(
        NestedField(1, "dlq_id", StringType(), required=True),
        NestedField(2, "source_topic", StringType(), required=True),
        NestedField(3, "document_id", StringType(), required=False),
        NestedField(4, "error_code", StringType(), required=True),
        NestedField(5, "error_message", StringType(), required=True),
        NestedField(6, "quarantined_at", TimestampType(), required=True),
        NestedField(7, "raw_payload_b64", StringType(), required=True),
    )


def _silver_doc_scd2_schema() -> Schema:
    return Schema(
        NestedField(1, "version_id", StringType(), required=True),
        NestedField(2, "document_id", StringType(), required=True),
        NestedField(3, "version_num", IntegerType(), required=True),
        NestedField(4, "title", StringType(), required=True),
        NestedField(5, "modality", StringType(), required=True),
        NestedField(6, "source_connector", StringType(), required=True),
        NestedField(7, "source_uri", StringType(), required=True),
        NestedField(8, "content_sha256", StringType(), required=True),
        NestedField(9, "detected_language", StringType(), required=True),
        NestedField(10, "security_tier", StringType(), required=True),
        NestedField(11, "acl_allow_csv", StringType(), required=True),
        NestedField(12, "acl_deny_csv", StringType(), required=True),
        NestedField(13, "pii_redacted_count", IntegerType(), required=True),
        NestedField(14, "pii_types_csv", StringType(), required=True),
        NestedField(15, "prompt_injection_flagged", BooleanType(), required=True),
        NestedField(16, "parser_engine", StringType(), required=True),
        NestedField(17, "quality_score", DoubleType(), required=True),
        NestedField(18, "effective_from", TimestampType(), required=True),
        NestedField(19, "effective_to", TimestampType(), required=False),
        NestedField(20, "is_current", BooleanType(), required=True),
        NestedField(21, "is_deleted", BooleanType(), required=True),
    )


def _silver_chunks_schema() -> Schema:
    return Schema(
        NestedField(1, "chunk_id", StringType(), required=True),
        NestedField(2, "parent_chunk_id", StringType(), required=True),
        NestedField(3, "document_id", StringType(), required=True),
        NestedField(4, "version_id", StringType(), required=True),
        NestedField(5, "version_num", IntegerType(), required=True),
        NestedField(6, "chunk_index", IntegerType(), required=True),
        NestedField(7, "strategy", StringType(), required=True),
        NestedField(8, "title", StringType(), required=True),
        NestedField(9, "modality", StringType(), required=True),
        NestedField(10, "language", StringType(), required=True),
        NestedField(11, "source_uri", StringType(), required=True),
        NestedField(12, "char_start", IntegerType(), required=True),
        NestedField(13, "char_end", IntegerType(), required=True),
        NestedField(14, "token_count", IntegerType(), required=True),
        NestedField(15, "chunk_text", StringType(), required=True),
        NestedField(16, "chunk_sha256", StringType(), required=True),
        NestedField(17, "security_tier", StringType(), required=True),
        NestedField(18, "acl_allow_csv", StringType(), required=True),
        NestedField(19, "acl_deny_csv", StringType(), required=True),
        NestedField(20, "pii_redacted", BooleanType(), required=True),
        NestedField(21, "prompt_injection_flagged", BooleanType(), required=True),
        NestedField(22, "is_active", BooleanType(), required=True),
        NestedField(23, "event_time", TimestampType(), required=True),
    )


def _silver_tombstones_schema() -> Schema:
    return Schema(
        NestedField(1, "tombstone_id", StringType(), required=True),
        NestedField(2, "document_id", StringType(), required=True),
        NestedField(3, "reason", StringType(), required=True),
        NestedField(4, "requested_by", StringType(), required=True),
        NestedField(5, "requested_at", TimestampType(), required=True),
        NestedField(6, "propagated_at", TimestampType(), required=True),
        NestedField(7, "chunks_purged", IntegerType(), required=True),
        NestedField(8, "propagation_ms", DoubleType(), required=True),
        NestedField(9, "sla_met", BooleanType(), required=True),
    )


def _gold_eval_schema() -> Schema:
    return Schema(
        NestedField(1, "eval_run_id", StringType(), required=True),
        NestedField(2, "executed_at", TimestampType(), required=True),
        NestedField(3, "mode", StringType(), required=True),
        NestedField(4, "model_version", StringType(), required=True),
        NestedField(5, "recall_at_1", DoubleType(), required=True),
        NestedField(6, "recall_at_5", DoubleType(), required=True),
        NestedField(7, "mrr_at_5", DoubleType(), required=True),
        NestedField(8, "ndcg_at_5", DoubleType(), required=True),
        NestedField(9, "groundedness", DoubleType(), required=True),
        NestedField(10, "acl_leakage_count", IntegerType(), required=True),
        NestedField(11, "prompt_injection_leaks", IntegerType(), required=True),
        NestedField(12, "deleted_doc_leaks", IntegerType(), required=True),
        NestedField(13, "p50_latency_ms", DoubleType(), required=True),
        NestedField(14, "p95_latency_ms", DoubleType(), required=True),
    )


def _quality_dq_schema() -> Schema:
    return Schema(
        NestedField(1, "run_id", StringType(), required=True),
        NestedField(2, "executed_at", TimestampType(), required=True),
        NestedField(3, "layer", StringType(), required=True),
        NestedField(4, "check_name", StringType(), required=True),
        NestedField(5, "status", StringType(), required=True),
        NestedField(6, "observed_value", DoubleType(), required=True),
        NestedField(7, "threshold", DoubleType(), required=True),
        NestedField(8, "details_json", StringType(), required=True),
    )


class MultimodalLakehouse:
    """Governed Apache Iceberg V2 Lakehouse orchestrating Ingestion -> Parsing -> Chunking -> Indexing -> Deletion -> DQ."""

    CONSUMER_GROUP = "multimodal-lakehouse-ingestor"

    def __init__(self, config: LakehouseConfig) -> None:
        self.config = config
        self.config.ensure_local_dirs()
        self.logger = StructuredLogger("multimodal-lakehouse", "iceberg_engine", environment=config.env)
        self.metrics = get_metrics()
        self.lineage = OpenLineageEmitter(
            namespace=config.openlineage_namespace,
            marquez_url=config.openlineage_url,
            local_dir=config.lineage_dir,
        )
        self.catalog: Catalog = load_catalog(
            "multimodal_local",
            **{
                "type": "sql",
                "uri": f"sqlite:///{config.sqlite_catalog_path}",
                "warehouse": f"file://{config.warehouse_dir}",
            },
        )
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        for ns in ("bronze", "silver", "gold", "quality"):
            try:
                self.catalog.create_namespace(ns)
            except NamespaceAlreadyExistsError:
                pass

        specs: list[tuple[str, Schema, PartitionSpec | None]] = [
            (
                "bronze.raw_assets",
                _bronze_assets_schema(),
                PartitionSpec(PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="event_day")),
            ),
            ("bronze.quarantine_dlq", _bronze_quarantine_schema(), None),
            ("silver.document_versions_scd2", _silver_doc_scd2_schema(), None),
            (
                "silver.chunks_lineage",
                _silver_chunks_schema(),
                PartitionSpec(PartitionField(source_id=23, field_id=1001, transform=DayTransform(), name="chunk_day")),
            ),
            ("silver.deletion_tombstones", _silver_tombstones_schema(), None),
            ("gold.retrieval_corpus_active", _silver_chunks_schema(), None),
            ("gold.rag_eval_metrics", _gold_eval_schema(), None),
            ("quality.dq_check_results", _quality_dq_schema(), None),
        ]
        for ident, schema, pspec in specs:
            try:
                self.catalog.load_table(ident)
            except NoSuchTableError:
                kwargs: dict[str, Any] = {"schema": schema, "properties": {"format-version": "2"}}
                if pspec is not None:
                    kwargs["partition_spec"] = pspec
                self.catalog.create_table(ident, **kwargs)

    def load_table(self, ident: str) -> Table:
        return self.catalog.load_table(ident)

    def append_pylist(self, ident: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        tbl = self.load_table(ident)
        tbl.append(pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow()))
        return len(rows)

    def overwrite_pylist(self, ident: str, rows: list[dict[str, Any]]) -> int:
        tbl = self.load_table(ident)
        tbl.overwrite(pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow()))
        return len(rows)

    def read_arrow(self, ident: str, *, snapshot_id: int | None = None) -> pa.Table:
        tbl = self.load_table(ident)
        return (tbl.scan(snapshot_id=snapshot_id) if snapshot_id else tbl.scan()).to_arrow()

    def query_sql(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        con = duckdb.connect(":memory:")
        try:
            mapping = {
                "bronze_raw_assets": "bronze.raw_assets",
                "bronze_quarantine": "bronze.quarantine_dlq",
                "silver_docs_scd2": "silver.document_versions_scd2",
                "silver_chunks": "silver.chunks_lineage",
                "silver_tombstones": "silver.deletion_tombstones",
                "gold_active_corpus": "gold.retrieval_corpus_active",
                "gold_eval_metrics": "gold.rag_eval_metrics",
                "quality_dq": "quality.dq_check_results",
            }
            for alias, ident in mapping.items():
                if alias in sql:
                    con.register(alias, self.read_arrow(ident))
            cur = con.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        finally:
            con.close()

    def table_stats(self, ident: str) -> dict[str, Any]:
        tbl = self.load_table(ident)
        snaps = list(tbl.snapshots())
        cur = tbl.current_snapshot()
        tasks = list(tbl.scan().plan_files()) if cur else []
        total_bytes = sum(t.file.file_size_in_bytes for t in tasks)
        total_recs = sum(t.file.record_count for t in tasks)
        return {
            "table": ident,
            "format_version": tbl.metadata.format_version,
            "snapshot_count": len(snaps),
            "current_snapshot_id": cur.snapshot_id if cur else None,
            "data_file_count": len(tasks),
            "total_records": total_recs,
            "total_bytes": total_bytes,
        }

    def all_table_stats(self) -> list[dict[str, Any]]:
        return [
            self.table_stats(n)
            for n in (
                "bronze.raw_assets",
                "bronze.quarantine_dlq",
                "silver.document_versions_scd2",
                "silver.chunks_lineage",
                "silver.deletion_tombstones",
                "gold.retrieval_corpus_active",
                "gold.rag_eval_metrics",
                "quality.dq_check_results",
            )
        ]

    def compact_table(self, ident: str) -> dict[str, Any]:
        t0 = time.perf_counter()
        before = self.table_stats(ident)
        if before["data_file_count"] <= 1 or before["total_records"] == 0:
            return {"table": ident, "action": "skipped", "files_before": before["data_file_count"], "files_after": before["data_file_count"]}
        arr = self.read_arrow(ident)
        self.load_table(ident).overwrite(arr)
        after = self.table_stats(ident)
        return {
            "table": ident,
            "action": "compacted",
            "files_before": before["data_file_count"],
            "files_after": after["data_file_count"],
            "bytes_before": before["total_bytes"],
            "bytes_after": after["total_bytes"],
            "records_preserved": after["total_records"],
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        }

    def process_from_bus(
        self,
        bus: EventBus,
        registry: AvroContractRegistry,
        index: HybridRetrievalIndex,
        *,
        chunk_strategy: str = "semantic",
        run_migration_v2: bool = True,
    ) -> dict[str, Any]:
        """Run full Ingestion -> Parsing DAG -> SCD2 Revision & Stale Guard -> Chunking -> Dual Indexing -> Tombstone Propagation -> Eval."""
        t0 = time.perf_counter()
        run_id = str(uuid.uuid4())
        self.lineage.emit(
            event_type="START",
            job_name="multimodal_ingestion_parse_index",
            run_id=run_id,
            inputs=[{"name": "multimodal.source.events"}, {"name": "multimodal.deletions.tombstones"}],
            outputs=[
                {"name": "bronze.raw_assets"},
                {"name": "silver.document_versions_scd2"},
                {"name": "silver.chunks_lineage"},
                {"name": "gold.retrieval_corpus_active"},
            ],
        )

        bronze_rows: list[dict[str, Any]] = []
        quarantine_rows: list[dict[str, Any]] = []
        seen_dedup: set[tuple[str, int, str]] = set()
        duplicates_suppressed = 0
        stale_revisions_rejected = 0

        # Track authoritative per-document version timeline for SCD2 & stale out-of-order rejection
        doc_history: dict[str, list[dict[str, Any]]] = {}
        chunks_by_version: dict[str, list[VersionedChunk]] = {}

        recs = bus.read_range("multimodal.source.events")
        for rec in recs:
            try:
                _, ev = registry.deserialize(rec.value, expected_topic="multimodal.source.events")
            except SchemaValidationError as exc:
                q_row = {
                    "dlq_id": str(uuid.uuid4()),
                    "source_topic": rec.topic,
                    "document_id": rec.key,
                    "error_code": "AVRO_SCHEMA_INVALID",
                    "error_message": str(exc)[:380],
                    "quarantined_at": datetime.now(UTC).replace(tzinfo=None),
                    "raw_payload_b64": base64.b64encode(rec.value).decode("ascii"),
                }
                quarantine_rows.append(q_row)
                self.metrics.quarantined_assets_total.labels(reason="AVRO_SCHEMA_INVALID").inc()
                bus.produce(
                    "multimodal.dead-letter",
                    rec.key,
                    registry.serialize(
                        "multimodal.dead-letter",
                        {
                            **q_row,
                            "quarantined_at": datetime.now(UTC).isoformat(),
                            "trace_id": "dlq-trace",
                            "schema_version": "1.0.0",
                        },
                    ),
                )
                continue

            doc_id = str(ev["document_id"])
            ver_num = int(ev["version_num"])
            sha = str(ev["content_sha256"])
            dedup_key = (doc_id, ver_num, sha)
            if dedup_key in seen_dedup:
                duplicates_suppressed += 1
                continue
            seen_dedup.add(dedup_key)

            ev_dt = _parse_dt(ev["event_time"])
            bronze_rows.append(
                {
                    "event_id": ev["event_id"],
                    "document_id": doc_id,
                    "version_num": ver_num,
                    "event_time": ev_dt,
                    "ingestion_time": _parse_dt(ev["ingestion_time"]),
                    "source_connector": ev["source_connector"],
                    "source_uri": ev["source_uri"],
                    "modality": ev["modality"],
                    "mime_type": ev["mime_type"],
                    "content_sha256": sha,
                    "byte_size": int(ev["byte_size"]),
                    "title": ev["title"],
                    "security_tier": ev["security_tier"],
                    "acl_allow_csv": ",".join(ev.get("acl_allow_groups") or []),
                    "acl_deny_csv": ",".join(ev.get("acl_deny_groups") or []),
                }
            )

            # Out-of-order revision guard: reject older version_num OR older event_time arriving after a newer version!
            existing = doc_history.get(doc_id, [])
            if existing:
                latest = existing[-1]
                if ver_num < latest["version_num"] or (ver_num == latest["version_num"] and ev_dt < latest["effective_from"]):
                    stale_revisions_rejected += 1
                    continue

            # Parse real binary artifact; quarantine corrupt media!
            try:
                art = parse_multimodal_asset(ev)
            except MediaCorruptError as exc:
                q_row = {
                    "dlq_id": str(uuid.uuid4()),
                    "source_topic": rec.topic,
                    "document_id": doc_id,
                    "error_code": "MEDIA_CORRUPT",
                    "error_message": str(exc)[:380],
                    "quarantined_at": datetime.now(UTC).replace(tzinfo=None),
                    "raw_payload_b64": base64.b64encode(str(ev.get("payload_path", "")).encode("utf-8")).decode("ascii"),
                }
                quarantine_rows.append(q_row)
                self.metrics.quarantined_assets_total.labels(reason="MEDIA_CORRUPT").inc()
                continue

            # Close previous SCD2 version
            if existing:
                existing[-1]["effective_to"] = ev_dt
                existing[-1]["is_current"] = False

            version_id = f"{doc_id}:v{ver_num}:{sha[:10]}"
            scd_row = {
                "version_id": version_id,
                "document_id": doc_id,
                "version_num": ver_num,
                "title": ev["title"],
                "modality": art.modality,
                "source_connector": ev["source_connector"],
                "source_uri": ev["source_uri"],
                "content_sha256": sha,
                "detected_language": art.detected_language,
                "security_tier": ev["security_tier"],
                "acl_allow_csv": ",".join(sorted(set(ev.get("acl_allow_groups") or []))),
                "acl_deny_csv": ",".join(sorted(set(ev.get("acl_deny_groups") or []))),
                "pii_redacted_count": art.pii_redacted_count,
                "pii_types_csv": ",".join(art.pii_entities_found),
                "prompt_injection_flagged": art.prompt_injection_detected,
                "parser_engine": art.parser_engine,
                "quality_score": art.quality_score,
                "effective_from": ev_dt,
                "effective_to": None,
                "is_current": True,
                "is_deleted": False,
            }
            doc_history.setdefault(doc_id, []).append(scd_row)
            chunks_by_version[version_id] = chunk_artifact(ev, art, strategy=chunk_strategy)

        # Process deletion tombstones from durable topic
        tombstone_rows: list[dict[str, Any]] = []
        deleted_doc_ids: set[str] = set()
        for trec in bus.read_range("multimodal.deletions.tombstones"):
            _, tomb = registry.deserialize(trec.value, expected_topic="multimodal.deletions.tombstones")
            del_doc = str(tomb["document_id"])
            deleted_doc_ids.add(del_doc)
            if del_doc in doc_history:
                for row in doc_history[del_doc]:
                    row["is_current"] = False
                    row["is_deleted"] = True
                    if row["effective_to"] is None:
                        row["effective_to"] = _parse_dt(tomb["requested_at"])
            tombstone_rows.append(
                {
                    "tombstone_id": tomb["tombstone_id"],
                    "document_id": del_doc,
                    "reason": tomb["reason"],
                    "requested_by": tomb["requested_by"],
                    "requested_at": _parse_dt(tomb["requested_at"]),
                    "propagated_at": datetime.now(UTC).replace(tzinfo=None),
                    "chunks_purged": sum(len(chunks_by_version.get(r["version_id"], [])) for r in doc_history.get(del_doc, [])),
                    "propagation_ms": 0.0,
                    "sla_met": True,
                }
            )
            self.metrics.deletions_propagated_total.labels(status="PROPAGATED").inc()

        # Flatten Silver SCD2 & Chunks + Gold Active Corpus (strictly current, non-deleted documents)
        scd2_rows = [r for hist in doc_history.values() for r in hist]
        current_active_version_ids = {r["version_id"] for r in scd2_rows if r["is_current"] and not r["is_deleted"]}

        silver_chunk_rows: list[dict[str, Any]] = []
        gold_active_chunks: list[VersionedChunk] = []
        gold_chunk_rows: list[dict[str, Any]] = []

        for ver_id, clist in chunks_by_version.items():
            active = ver_id in current_active_version_ids
            for c in clist:
                row = {
                    "chunk_id": c.chunk_id,
                    "parent_chunk_id": c.parent_chunk_id,
                    "document_id": c.document_id,
                    "version_id": c.version_id,
                    "version_num": c.version_num,
                    "chunk_index": c.chunk_index,
                    "strategy": c.strategy,
                    "title": c.title,
                    "modality": c.modality,
                    "language": c.language,
                    "source_uri": c.source_uri,
                    "char_start": c.char_start,
                    "char_end": c.char_end,
                    "token_count": c.token_count,
                    "chunk_text": c.chunk_text,
                    "chunk_sha256": c.chunk_sha256,
                    "security_tier": c.security_tier,
                    "acl_allow_csv": ",".join(c.acl_allow_groups),
                    "acl_deny_csv": ",".join(c.acl_deny_groups),
                    "pii_redacted": c.pii_redacted,
                    "prompt_injection_flagged": c.prompt_injection_flagged,
                    "is_active": active,
                    "event_time": _parse_dt(c.event_time),
                }
                silver_chunk_rows.append(row)
                if active:
                    gold_active_chunks.append(c)
                    gold_chunk_rows.append(row)

        # Write Iceberg V2 Medallion tables
        self.append_pylist("bronze.raw_assets", bronze_rows)
        self.append_pylist("bronze.quarantine_dlq", quarantine_rows)
        self.overwrite_pylist("silver.document_versions_scd2", scd2_rows)
        self.overwrite_pylist("silver.chunks_lineage", silver_chunk_rows)
        self.overwrite_pylist("silver.deletion_tombstones", tombstone_rows)
        self.overwrite_pylist("gold.retrieval_corpus_active", gold_chunk_rows)

        # Build v1 embeddings, then optionally execute live blue/green v2 re-embedding migration
        idx_v1 = index.build_index(gold_active_chunks, model_version="embed-v1-tfidf-svd-128d", activate=True)
        migration_res = (
            index.migrate_reembed("embed-v2-charword-svd-256d") if run_migration_v2 else None
        )

        # Propagate tombstones against index/cache explicitly to confirm zero residual deleted chunks
        for del_doc in deleted_doc_ids:
            p_res = index.purge_document(del_doc)
            for tr in tombstone_rows:
                if tr["document_id"] == del_doc:
                    tr["propagation_ms"] = max(0.5, float(p_res["purge_duration_ms"]))
        if tombstone_rows:
            self.overwrite_pylist("silver.deletion_tombstones", tombstone_rows)

        # Run Reproducible Evaluation Suite across retrieval modes & persist to gold.rag_eval_metrics
        eval_runs = run_evaluation_suite(index)
        eval_run_id = str(uuid.uuid4())
        now_dt = datetime.now(UTC).replace(tzinfo=None)
        eval_rows = [
            {
                "eval_run_id": eval_run_id,
                "executed_at": now_dt,
                "mode": er["mode"],
                "model_version": er["model_version"],
                "recall_at_1": er["recall_at_1"],
                "recall_at_5": er["recall_at_5"],
                "mrr_at_5": er["mrr_at_5"],
                "ndcg_at_5": er["ndcg_at_5"],
                "groundedness": er["groundedness"],
                "acl_leakage_count": er["acl_leakage_count"],
                "prompt_injection_leaks": er["prompt_injection_leaks"],
                "deleted_doc_leaks": er["deleted_doc_leaks"],
                "p50_latency_ms": er["p50_latency_ms"],
                "p95_latency_ms": er["p95_latency_ms"],
            }
            for er in eval_runs
        ]
        self.append_pylist("gold.rag_eval_metrics", eval_rows)

        # Commit consumer offsets cleanly so topic lag reaches 0
        part_offs: dict[int, int] = {}
        for r in recs:
            part_offs[r.partition] = max(part_offs.get(r.partition, 0), r.offset + 1)
        if part_offs:
            bus.commit_offsets("multimodal.source.events", self.CONSUMER_GROUP, part_offs)

        self.lineage.emit(
            event_type="COMPLETE",
            job_name="multimodal_ingestion_parse_index",
            run_id=run_id,
            outputs=[
                {"name": "bronze.raw_assets", "row_count": len(bronze_rows)},
                {"name": "silver.document_versions_scd2", "row_count": len(scd2_rows)},
                {"name": "gold.retrieval_corpus_active", "row_count": len(gold_chunk_rows)},
            ],
        )

        return {
            "bronze_assets": len(bronze_rows),
            "quarantined_dlq": len(quarantine_rows),
            "duplicates_suppressed": duplicates_suppressed,
            "stale_revisions_rejected": stale_revisions_rejected,
            "silver_scd2_versions": len(scd2_rows),
            "silver_total_chunks": len(silver_chunk_rows),
            "gold_active_chunks": len(gold_chunk_rows),
            "tombstones_propagated": len(tombstone_rows),
            "index_v1": idx_v1,
            "migration_v2": migration_res,
            "evaluations": eval_runs,
            "duration_seconds": round(time.perf_counter() - t0, 4),
        }

    def propagate_deletion(
        self,
        document_id: str,
        *,
        reason: str,
        requested_by: str,
        bus: EventBus,
        registry: AvroContractRegistry,
        index: HybridRetrievalIndex,
    ) -> dict[str, Any]:
        """On-demand right-to-be-forgotten deletion propagating across Kafka, Iceberg Silver/Gold, vector/BM25 indexes & query cache."""
        t0 = time.perf_counter()
        now_iso = datetime.now(UTC).isoformat()
        now_dt = datetime.now(UTC).replace(tzinfo=None)
        tomb_id = str(uuid.uuid4())
        tomb_ev = {
            "tombstone_id": tomb_id,
            "document_id": document_id,
            "reason": reason,
            "requested_by": requested_by,
            "requested_at": now_iso,
            "sla_deadline_seconds": 60,
            "trace_id": uuid.uuid4().hex,
            "schema_version": "1.0.0",
        }
        bus.produce(
            "multimodal.deletions.tombstones",
            document_id,
            registry.serialize("multimodal.deletions.tombstones", tomb_ev),
        )

        # 1. Mark document deleted in Silver SCD2
        scd_rows = self.read_arrow("silver.document_versions_scd2").to_pylist()
        for r in scd_rows:
            if r["document_id"] == document_id:
                r["is_current"] = False
                r["is_deleted"] = True
                if r["effective_to"] is None:
                    r["effective_to"] = now_dt
        self.overwrite_pylist("silver.document_versions_scd2", scd_rows)

        # 2. Deactivate in Silver chunks and remove completely from Gold active corpus
        s_chunks = self.read_arrow("silver.chunks_lineage").to_pylist()
        for c in s_chunks:
            if c["document_id"] == document_id:
                c["is_active"] = False
        self.overwrite_pylist("silver.chunks_lineage", s_chunks)

        g_chunks = [c for c in self.read_arrow("gold.retrieval_corpus_active").to_pylist() if c["document_id"] != document_id]
        self.overwrite_pylist("gold.retrieval_corpus_active", g_chunks)

        # 3. Purge BM25, ALL vector index versions, and query cache
        idx_purge = index.purge_document(document_id)
        dur_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        tomb_rec = {
            "tombstone_id": tomb_id,
            "document_id": document_id,
            "reason": reason,
            "requested_by": requested_by,
            "requested_at": now_dt,
            "propagated_at": datetime.now(UTC).replace(tzinfo=None),
            "chunks_purged": int(idx_purge["chunks_purged"]),
            "propagation_ms": dur_ms,
            "sla_met": dur_ms < 60_000.0,
        }
        self.append_pylist("silver.deletion_tombstones", [tomb_rec])
        self.metrics.deletions_propagated_total.labels(status="PROPAGATED").inc()
        return {**tomb_rec, "requested_at": now_iso, "propagated_at": datetime.now(UTC).isoformat(), "cache_invalidated": True}

    def run_data_quality(self, index: HybridRetrievalIndex) -> dict[str, Any]:
        """Run 10-check multimodal governance, PII, ACL, lineage, and erasure Data Quality Suite."""
        run_id = str(uuid.uuid4())
        now = datetime.now(UTC).replace(tzinfo=None)
        checks: list[dict[str, Any]] = []

        def rec(layer: str, name: str, passed: bool, obs: float, th: float, details: dict[str, Any]) -> None:
            st = "PASS" if passed else "FAIL"
            checks.append(
                {
                    "run_id": run_id,
                    "executed_at": now,
                    "layer": layer,
                    "check_name": name,
                    "status": st,
                    "observed_value": round(obs, 4),
                    "threshold": th,
                    "details_json": json.dumps(details, default=str),
                }
            )
            self.metrics.dq_checks_total.labels(check_name=name, status=st).inc()

        # 1. Zero unredacted SSNs in Gold active retrieval corpus
        ssn_leaks = self.query_sql(
            "SELECT COUNT(*) AS cnt FROM gold_active_corpus WHERE regexp_matches(chunk_text, '[0-9]{3}-[0-9]{2}-[0-9]{4}')"
        )[0]["cnt"]
        rec("privacy", "zero_raw_ssn_in_active_corpus", ssn_leaks == 0, ssn_leaks, 0.0, {"ssn_leaks": ssn_leaks})

        # 2. Zero deleted documents present in Gold active corpus
        del_leaks = self.query_sql(
            "SELECT COUNT(*) AS cnt FROM gold_active_corpus g JOIN silver_tombstones t ON g.document_id = t.document_id"
        )[0]["cnt"]
        rec("deletion", "zero_deleted_docs_in_gold_corpus", del_leaks == 0, del_leaks, 0.0, {"deleted_leaks": del_leaks})

        # 3. Zero deleted documents in live hybrid index
        tomb_ids = {r["document_id"] for r in self.query_sql("SELECT DISTINCT document_id FROM silver_tombstones")}
        idx_del_leaks = sum(1 for c in index.chunks if c["document_id"] in tomb_ids)
        rec("deletion", "zero_deleted_docs_in_live_index", idx_del_leaks == 0, idx_del_leaks, 0.0, {"index_leaks": idx_del_leaks})

        # 4. Chunk ID uniqueness in Gold active corpus
        c_dups = self.query_sql("SELECT COUNT(*) - COUNT(DISTINCT chunk_id) AS d FROM gold_active_corpus")[0]["d"]
        rec("gold", "chunk_id_uniqueness", c_dups == 0, c_dups, 0.0, {"duplicates": c_dups})

        # 5. SCD2 at most one is_current row per document_id
        scd_dups = self.query_sql(
            "SELECT COUNT(*) AS cnt FROM (SELECT document_id FROM silver_docs_scd2 WHERE is_current GROUP BY 1 HAVING COUNT(*) > 1)"
        )[0]["cnt"]
        rec("silver", "scd2_single_current_version_per_doc", scd_dups == 0, scd_dups, 0.0, {"violations": scd_dups})

        # 6. Referential chunk -> SCD2 version lineage completeness
        orphans = self.query_sql(
            "SELECT COUNT(*) AS cnt FROM gold_active_corpus g LEFT JOIN silver_docs_scd2 s ON g.version_id = s.version_id WHERE s.version_id IS NULL"
        )[0]["cnt"]
        rec("lineage", "chunk_to_version_lineage_integrity", orphans == 0, orphans, 0.0, {"orphan_chunks": orphans})

        # 7. Multimodal coverage (all 5 modalities represented: pdf, markdown, image, audio, structured)
        mods = self.query_sql("SELECT COUNT(DISTINCT modality) AS cnt FROM gold_active_corpus")[0]["cnt"]
        rec("corpus", "multimodal_coverage_all_5_types", mods >= 5, mods, 5.0, {"modalities_present": mods})

        # 8. Gold Iceberg <-> Live Hybrid Index exact chunk parity
        gold_cnt = self.query_sql("SELECT COUNT(*) AS cnt FROM gold_active_corpus")[0]["cnt"]
        diff = abs(int(gold_cnt) - len(index.chunks))
        rec("reconciliation", "gold_iceberg_to_index_chunk_parity", diff == 0, diff, 0.0, {"gold_chunks": gold_cnt, "index_chunks": len(index.chunks)})

        # 9. Zero ACL leakage & zero prompt-injection leakage in latest eval run
        eval_leaks = self.query_sql(
            "SELECT COALESCE(SUM(acl_leakage_count + prompt_injection_leaks + deleted_doc_leaks), 0) AS cnt FROM gold_eval_metrics WHERE eval_run_id = (SELECT eval_run_id FROM gold_eval_metrics ORDER BY executed_at DESC LIMIT 1)"
        )[0]["cnt"]
        rec("security", "zero_acl_and_injection_eval_leakage", eval_leaks == 0, eval_leaks, 0.0, {"security_leaks": eval_leaks})

        # 10. Quarantine / DLQ auditability (corrupt media & poison Avro safely captured, zero pipeline crash)
        q_cnt = self.query_sql("SELECT COUNT(*) AS cnt FROM bronze_quarantine")[0]["cnt"]
        rec("bronze", "corrupt_media_quarantine_captured", q_cnt >= 1, q_cnt, 1.0, {"quarantined_records": q_cnt})

        self.append_pylist("quality.dq_check_results", checks)
        passed = sum(1 for c in checks if c["status"] == "PASS")
        return {
            "run_id": run_id,
            "executed_at": now.isoformat(),
            "overall_status": "PASS" if passed == len(checks) else "FAIL",
            "summary": {"passed": passed, "failed": len(checks) - passed, "total": len(checks)},
            "checks": checks,
        }
