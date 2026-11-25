"""Apache Airflow 3.4.0 DAG Suite for AI-Native Multimodal Lakehouse & RAG Governance."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

try:
    from airflow.decorators import dag, task
except ImportError:  # pragma: no cover
    def dag(*args: Any, **kwargs: Any):  # type: ignore[misc]
        def _w(fn: Any) -> Any:
            return fn
        return _w

    def task(*args: Any, **kwargs: Any):  # type: ignore[misc]
        def _w(fn: Any) -> Any:
            return fn
        return _w


DEFAULT_ARGS = {"owner": "ai-data-platform", "retries": 2, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="multimodal_ingest_parse_and_index",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["multimodal", "iceberg", "rag"],
)
def multimodal_ingest_parse_and_index() -> None:
    @task
    def parse_chunk_and_index() -> dict[str, Any]:
        from multimodal_lakehouse.config import load_config
        from multimodal_lakehouse.contracts import AvroContractRegistry
        from multimodal_lakehouse.lakehouse import MultimodalLakehouse
        from multimodal_lakehouse.retrieval import HybridRetrievalIndex
        from multimodal_lakehouse.transport import create_event_bus

        cfg = load_config()
        lh = MultimodalLakehouse(cfg)
        idx = HybridRetrievalIndex(cfg.index_dir)
        res = lh.process_from_bus(create_event_bus(cfg), AvroContractRegistry(cfg.project_root / "schemas" / "avro"), idx)
        dq = lh.run_data_quality(idx)
        if dq["overall_status"] != "PASS":
            raise RuntimeError(f"Multimodal DQ failed: {dq['summary']}")
        return {"active_chunks": res["gold_active_chunks"], "dq": dq["summary"]}

    parse_chunk_and_index()


@dag(
    dag_id="multimodal_reembed_migration_and_eval",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["multimodal", "embeddings", "evaluation"],
)
def multimodal_reembed_migration_and_eval() -> None:
    @task
    def migrate_and_gate() -> dict[str, Any]:
        from multimodal_lakehouse.config import load_config
        from multimodal_lakehouse.evaluation import evaluate_retrieval_mode
        from multimodal_lakehouse.retrieval import HybridRetrievalIndex

        cfg = load_config()
        idx = HybridRetrievalIndex(cfg.index_dir)
        idx.load()
        mig = idx.migrate_reembed("embed-v2-charword-svd-256d")
        ev = evaluate_retrieval_mode(idx, mode="hybrid_rerank", model_version="embed-v2-charword-svd-256d")
        if ev["acl_leakage_count"] > 0 or ev["deleted_doc_leaks"] > 0:
            raise RuntimeError("Security gate failed during re-embedding evaluation!")
        return {"migration": mig, "eval": ev}

    migrate_and_gate()


@dag(
    dag_id="multimodal_iceberg_compaction",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["multimodal", "iceberg", "maintenance"],
)
def multimodal_iceberg_compaction() -> None:
    @task
    def compact() -> list[dict[str, Any]]:
        from multimodal_lakehouse.config import load_config
        from multimodal_lakehouse.lakehouse import MultimodalLakehouse

        lh = MultimodalLakehouse(load_config())
        return [lh.compact_table("bronze.raw_assets"), lh.compact_table("silver.chunks_lineage")]

    compact()


dag_ingest = multimodal_ingest_parse_and_index()
dag_reembed = multimodal_reembed_migration_and_eval()
dag_compact = multimodal_iceberg_compaction()
