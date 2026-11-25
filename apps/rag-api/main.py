"""FastAPI Permission-Aware Multimodal RAG Serving & Lakehouse Governance API (Port 8020)."""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field

_API_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _API_DIR.parent.parent
for _p in (str(_REPO_ROOT), str(_API_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from security import (  # noqa: E402
    AUDIT_TRAIL,
    PERSONA_PROFILES,
    record_audit,
    require_authenticated,
    resolve_principal,
)

from multimodal_lakehouse.config import LakehouseConfig, load_config  # noqa: E402
from multimodal_lakehouse.contracts import TOPIC_SPECS, AvroContractRegistry  # noqa: E402
from multimodal_lakehouse.lakehouse import MultimodalLakehouse  # noqa: E402
from multimodal_lakehouse.observability import get_metrics  # noqa: E402
from multimodal_lakehouse.retrieval import AccessPrincipal, HybridRetrievalIndex  # noqa: E402
from multimodal_lakehouse.transport import create_event_bus  # noqa: E402

DASHBOARD_DIR = _API_DIR.parent / "rag-dashboard"

app = FastAPI(
    title="AI-Native Multimodal Data Lakehouse & RAG Governance Platform",
    version="1.0.0",
    description="Governed multimodal ingestion, Iceberg V2 lineage, pre-retrieval ACL hybrid search, re-embedding migration, and deletion propagation API.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

_metrics = get_metrics()


class _Runtime:
    def __init__(self) -> None:
        self.cfg: LakehouseConfig = load_config()
        self.bus = create_event_bus(self.cfg)
        self.reg = AvroContractRegistry(self.cfg.project_root / "schemas" / "avro")
        self.lh = MultimodalLakehouse(self.cfg)
        self.idx = HybridRetrievalIndex(self.cfg.index_dir)
        self.idx.load()


_rt = _Runtime()


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next: Any) -> Response:
    t0 = time.perf_counter()
    resp: Response = await call_next(request)
    dur = time.perf_counter() - t0
    _metrics.api_requests_total.labels(
        endpoint=request.url.path, method=request.method, status_code=str(resp.status_code)
    ).inc()
    _metrics.api_latency_seconds.labels(endpoint=request.url.path).observe(dur)
    resp.headers["X-Response-Time-Ms"] = f"{dur * 1000.0:.2f}"
    resp.headers["X-Data-Notice"] = "Synthetic-Enterprise-Multimodal-Corpus"
    return resp


class RagQueryRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=500)
    top_k: int = Field(default=5, ge=1, le=20)
    mode: str = Field(default="hybrid_rerank")
    model_version: str | None = None
    modality: str | None = None
    language: str | None = None
    exclude_prompt_injections: bool = True


class DeleteDocRequest(BaseModel):
    reason: str = Field(default="GDPR_ARTICLE_17_ERASURE", min_length=3, max_length=120)


class MigrateRequest(BaseModel):
    target_model_version: str = Field(default="embed-v2-charword-svd-256d")


@app.get("/health")
def get_health() -> dict[str, Any]:
    tstats = _rt.lh.all_table_stats()
    return {
        "status": "healthy",
        "service": "ai-native-multimodal-lakehouse-api",
        "execution_profile": _rt.cfg.execution_profile,
        "active_model_version": _rt.idx.active_model_version,
        "indexed_chunks": len(_rt.idx.chunks),
        "iceberg_tables": len(tstats),
        "iceberg_total_records": sum(int(t["total_records"]) for t in tstats),
        "freshness_utc": datetime.now(UTC).isoformat(),
    }


@app.get("/metrics")
def get_prometheus_metrics() -> Response:
    return Response(content=_metrics.render_prometheus(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/summary")
def get_summary() -> dict[str, Any]:
    modalities = _rt.lh.query_sql(
        """
        SELECT modality, COUNT(DISTINCT document_id) AS docs, COUNT(*) AS chunks,
               SUM(CASE WHEN pii_redacted THEN 1 ELSE 0 END) AS pii_chunks,
               SUM(CASE WHEN prompt_injection_flagged THEN 1 ELSE 0 END) AS injection_chunks
        FROM gold_active_corpus GROUP BY 1 ORDER BY chunks DESC
        """
    )
    tiers = _rt.lh.query_sql(
        "SELECT security_tier, COUNT(DISTINCT document_id) AS docs, COUNT(*) AS chunks FROM gold_active_corpus GROUP BY 1 ORDER BY 1"
    )
    langs = _rt.lh.query_sql(
        "SELECT language, COUNT(*) AS chunks FROM gold_active_corpus GROUP BY 1 ORDER BY chunks DESC"
    )
    quarantine_cnt = _rt.lh.query_sql("SELECT COUNT(*) AS c FROM bronze_quarantine")[0]["c"]
    tomb_cnt = _rt.lh.query_sql("SELECT COUNT(*) AS c FROM silver_tombstones")[0]["c"]
    scd_cnt = _rt.lh.query_sql("SELECT COUNT(*) AS c FROM silver_docs_scd2")[0]["c"]
    lineage_path = _rt.cfg.lineage_dir / "openlineage_events.jsonl"
    ol_cnt = len(lineage_path.read_text(encoding="utf-8").splitlines()) if lineage_path.exists() else 0

    return {
        "kpis": {
            "active_documents": sum(int(m["docs"]) for m in modalities),
            "active_chunks": sum(int(m["chunks"]) for m in modalities),
            "scd2_versions": int(scd_cnt),
            "quarantined_assets": int(quarantine_cnt),
            "tombstones_propagated": int(tomb_cnt),
            "active_model_version": _rt.idx.active_model_version,
            "openlineage_events": ol_cnt,
        },
        "by_modality": modalities,
        "by_security_tier": tiers,
        "by_language": langs,
        "models": _rt.idx.model_catalog(),
        "freshness_utc": datetime.now(UTC).isoformat(),
    }


@app.post("/api/query")
def run_rag_query(
    payload: RagQueryRequest,
    principal: AccessPrincipal = require_authenticated("public_user"),
) -> dict[str, Any]:
    """Authenticated pre-retrieval ACL-filtered hybrid RAG query with cited provenance."""
    res = _rt.idx.query(
        payload.query,
        principal,
        top_k=payload.top_k,
        mode=payload.mode,
        model_version=payload.model_version,
        modality_filter=payload.modality,
        language_filter=payload.language,
        exclude_prompt_injections=payload.exclude_prompt_injections,
    )
    record_audit(
        principal,
        "rag_query",
        "/api/query",
        "ALLOWED",
        {
            "mode": payload.mode,
            "returned_hits": len(res["results"]),
            "acl_filtered_out": res["acl_filtered_out"],
            "injections_blocked": res["prompt_injections_blocked"],
        },
    )
    res["freshness_utc"] = datetime.now(UTC).isoformat()
    return res


@app.get("/api/demo-query")
def run_demo_query(
    q: str = Query(..., min_length=2),
    persona: str = Query(default="public_user"),
    mode: str = Query(default="hybrid_rerank"),
    modality: str | None = Query(default=None),
) -> dict[str, Any]:
    """Read-only interactive Control Tower demo switcher simulating personas strictly against synthetic corpus."""
    p = PERSONA_PROFILES.get(persona, PERSONA_PROFILES["public_user"])
    res = _rt.idx.query(q, p, top_k=5, mode=mode, modality_filter=modality or None, exclude_prompt_injections=True)
    record_audit(p, "demo_rag_query", "/api/demo-query", "ALLOWED", {"persona": persona, "hits": len(res["results"])})
    res["freshness_utc"] = datetime.now(UTC).isoformat()
    return res


@app.get("/api/documents")
def list_documents(limit: int = Query(default=40, ge=1, le=200)) -> dict[str, Any]:
    rows = _rt.lh.query_sql(
        f"""
        SELECT document_id, version_id, version_num, title, modality, source_connector,
               detected_language, security_tier, acl_allow_csv, acl_deny_csv,
               pii_redacted_count, prompt_injection_flagged, is_current, is_deleted, effective_from
        FROM silver_docs_scd2
        ORDER BY is_deleted ASC, version_num DESC, document_id ASC
        LIMIT {int(limit)}
        """
    )
    return {"documents": rows, "total": len(rows), "freshness_utc": datetime.now(UTC).isoformat()}


@app.get("/api/documents/{document_id}/lineage")
def get_document_lineage(document_id: str) -> dict[str, Any]:
    versions = _rt.lh.query_sql(
        "SELECT * FROM silver_docs_scd2 WHERE document_id = ? ORDER BY version_num ASC", [document_id]
    )
    if not versions:
        raise HTTPException(status_code=404, detail=f"Document '{document_id}' not found")
    chunks = _rt.lh.query_sql(
        """
        SELECT chunk_id, parent_chunk_id, version_id, version_num, chunk_index, strategy,
               char_start, char_end, token_count, chunk_sha256, security_tier, is_active, pii_redacted
        FROM silver_chunks WHERE document_id = ? ORDER BY version_num DESC, chunk_index ASC
        """,
        [document_id],
    )
    tombstones = _rt.lh.query_sql("SELECT * FROM silver_tombstones WHERE document_id = ?", [document_id])
    return {
        "document_id": document_id,
        "versions_scd2": versions,
        "chunks_lineage": chunks,
        "tombstones": tombstones,
        "freshness_utc": datetime.now(UTC).isoformat(),
    }


@app.delete("/api/documents/{document_id}")
def delete_document(
    document_id: str,
    body: DeleteDocRequest | None = None,
    principal: AccessPrincipal = require_authenticated("admin"),
) -> dict[str, Any]:
    """Admin-protected right-to-be-forgotten deletion propagating tombstones to Kafka, Iceberg, indexes & cache."""
    reason = body.reason if body else "GDPR_ARTICLE_17_ERASURE"
    res = _rt.lh.propagate_deletion(
        document_id,
        reason=reason,
        requested_by=principal.principal_id,
        bus=_rt.bus,
        registry=_rt.reg,
        index=_rt.idx,
    )
    record_audit(principal, "tombstone_delete", f"/api/documents/{document_id}", "PROPAGATED", {"chunks_purged": res["chunks_purged"]})
    return res


@app.get("/api/embeddings/models")
def get_embedding_models() -> dict[str, Any]:
    return {"active_model_version": _rt.idx.active_model_version, "models": _rt.idx.model_catalog()}


@app.post("/api/embeddings/migrate")
def trigger_reembed_migration(
    body: MigrateRequest,
    principal: AccessPrincipal = require_authenticated("eng_lead"),
) -> dict[str, Any]:
    res = _rt.idx.migrate_reembed(body.target_model_version)
    record_audit(principal, "reembed_migration", "/api/embeddings/migrate", "MIGRATED", res)
    return res


@app.get("/api/evaluations")
def get_evaluations() -> dict[str, Any]:
    rows = _rt.lh.query_sql(
        """
        SELECT eval_run_id, executed_at, mode, model_version, recall_at_1, recall_at_5,
               mrr_at_5, ndcg_at_5, groundedness, acl_leakage_count, prompt_injection_leaks,
               deleted_doc_leaks, p50_latency_ms, p95_latency_ms
        FROM gold_eval_metrics
        WHERE eval_run_id = (SELECT eval_run_id FROM gold_eval_metrics ORDER BY executed_at DESC LIMIT 1)
        ORDER BY mrr_at_5 DESC, p95_latency_ms ASC
        """
    )
    return {"evaluations": rows, "freshness_utc": datetime.now(UTC).isoformat()}


@app.get("/api/governance")
def get_governance() -> dict[str, Any]:
    dq = _rt.lh.query_sql(
        """
        SELECT layer, check_name, status, observed_value, threshold, details_json
        FROM quality_dq
        WHERE run_id = (SELECT run_id FROM quality_dq ORDER BY executed_at DESC LIMIT 1)
        ORDER BY layer, check_name
        """
    )
    quarantine = _rt.lh.query_sql(
        "SELECT dlq_id, source_topic, document_id, error_code, error_message, quarantined_at FROM bronze_quarantine ORDER BY quarantined_at DESC LIMIT 20"
    )
    tombstones = _rt.lh.query_sql("SELECT * FROM silver_tombstones ORDER BY requested_at DESC LIMIT 20")
    topics = [
        {
            "topic": t.name,
            "partitions": t.partitions,
            **_rt.bus.get_consumer_lag(t.name, MultimodalLakehouse.CONSUMER_GROUP),
        }
        for t in TOPIC_SPECS.values()
    ]
    chaos_file = _rt.cfg.project_root / "results" / "chaos" / "chaos_results.json"
    chaos = json.loads(chaos_file.read_text(encoding="utf-8")) if chaos_file.exists() else None
    passed = sum(1 for c in dq if c["status"] == "PASS")
    return {
        "dq_summary": {"passed": passed, "failed": len(dq) - passed, "total": len(dq), "overall_status": "PASS" if passed == len(dq) and dq else "FAIL"},
        "dq_checks": dq,
        "quarantine": quarantine,
        "tombstones": tombstones,
        "topics": topics,
        "iceberg_tables": _rt.lh.all_table_stats(),
        "audit_trail": list(reversed(AUDIT_TRAIL[-15:])),
        "chaos_summary": chaos,
        "freshness_utc": datetime.now(UTC).isoformat(),
    }


@app.get("/api/governance/audit")
def get_audit_trail(request: Request) -> dict[str, Any]:
    _ = resolve_principal(request)
    return {"events": list(reversed(AUDIT_TRAIL[-50:]))}


if (DASHBOARD_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(DASHBOARD_DIR / "assets")), name="assets")


@app.get("/", include_in_schema=False)
def serve_dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_DIR / "index.html")
