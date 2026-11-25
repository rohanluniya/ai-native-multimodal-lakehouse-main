"""End-to-end integration tests: deterministic generation, SCD2 revisions, ACL pre-filtering, Iceberg time travel/compaction, deletion propagation, and FastAPI RBAC."""

from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

from multimodal_lakehouse.config import LakehouseConfig
from multimodal_lakehouse.contracts import AvroContractRegistry
from multimodal_lakehouse.evaluation import ENG_LEAD, FINANCE_ANALYST, PUBLIC_USER
from multimodal_lakehouse.generator import MultimodalCorpusGenerator
from multimodal_lakehouse.lakehouse import MultimodalLakehouse
from multimodal_lakehouse.retrieval import HybridRetrievalIndex
from multimodal_lakehouse.transport import LocalWalEventBus


def test_e2e_multimodal_pipeline_acl_revisions_erasure_and_api(isolated_config: LakehouseConfig) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry(isolated_config.project_root / "schemas" / "avro")
    lh = MultimodalLakehouse(isolated_config)
    idx = HybridRetrievalIndex(isolated_config.index_dir)

    gen = MultimodalCorpusGenerator(isolated_config, bus, reg, profile="smoke", seed=2026).run()
    assert gen["counts"]["canonical_events"] == 13
    assert gen["counts"]["corrupt_injected"] >= 3

    proc = lh.process_from_bus(bus, reg, idx, chunk_strategy="semantic", run_migration_v2=True)
    assert proc["stale_revisions_rejected"] == 1
    assert proc["duplicates_suppressed"] >= 3
    assert proc["quarantined_dlq"] >= 5
    assert proc["migration_v2"]["active_model_version"] == "embed-v2-charword-svd-256d"

    # Verify DOC-ARCH-001 settled on revised v2 (14 days / 256MB), NOT stale late v1!
    arch_cur = lh.query_sql("SELECT version_num, title FROM silver_docs_scd2 WHERE document_id = 'DOC-ARCH-001' AND is_current")
    assert len(arch_cur) == 1 and arch_cur[0]["version_num"] == 2

    # Verify ACL pre-filtering: public cannot see DOC-FIN-003; eng_lead cannot see DOC-HR-004 (deny overrides allow!)
    pub_res = idx.query("Q4 revenue forecast executive compensation", PUBLIC_USER, top_k=5)
    assert all(h["document_id"] not in ("DOC-FIN-003", "DOC-HR-004") for h in pub_res["results"])

    eng_res = idx.query("Executive compensation CEO bonus equity refresh", ENG_LEAD, top_k=5)
    assert all(h["document_id"] != "DOC-HR-004" for h in eng_res["results"])

    fin_res = idx.query("Q4 enterprise revenue forecast gross margin", FINANCE_ANALYST, top_k=5)
    assert any(h["document_id"] == "DOC-FIN-003" for h in fin_res["results"])
    # Verify PII was redacted before embedding!
    assert "123-45-6789" not in fin_res["grounded_answer"]

    # Verify all 10 DQ checks pass & zero security leaks across every eval mode
    dq = lh.run_data_quality(idx)
    assert dq["overall_status"] == "PASS"
    assert dq["summary"]["passed"] == 10
    for ev in proc["evaluations"]:
        assert ev["acl_leakage_count"] == 0
        assert ev["prompt_injection_leaks"] == 0
        assert ev["deleted_doc_leaks"] == 0
        assert 0.0 <= ev["ndcg_at_5"] <= 1.0

    # Verify Iceberg time travel & compaction on bronze.raw_assets
    snap_1 = lh.load_table("bronze.raw_assets").current_snapshot().snapshot_id
    lh.append_pylist("bronze.raw_assets", lh.read_arrow("bronze.raw_assets").to_pylist())
    snap_2 = lh.load_table("bronze.raw_assets").current_snapshot().snapshot_id
    assert snap_1 != snap_2
    assert len(lh.read_arrow("bronze.raw_assets", snapshot_id=snap_2)) == 2 * len(lh.read_arrow("bronze.raw_assets", snapshot_id=snap_1))
    comp = lh.compact_table("bronze.raw_assets")
    assert comp["action"] == "compacted" and comp["files_after"] == 1

    # Verify FastAPI endpoints + RBAC enforcement + Live DELETE tombstone propagation
    api_dir = Path(isolated_config.project_root) / "apps" / "rag-api"
    if str(api_dir) not in sys.path:
        sys.path.insert(0, str(api_dir))
    import main as rag_main  # type: ignore[import-not-found]

    rag_main._rt.cfg = isolated_config
    rag_main._rt.bus = bus
    rag_main._rt.reg = reg
    rag_main._rt.lh = lh
    rag_main._rt.idx = idx

    client = TestClient(rag_main.app)
    for ep in ("/health", "/metrics", "/api/summary", "/api/documents", "/api/documents/DOC-ARCH-001/lineage", "/api/embeddings/models", "/api/evaluations", "/api/governance"):
        assert client.get(ep).status_code == 200, f"GET {ep} failed"

    # Unauthenticated POST /api/query rejected with 401
    assert client.post("/api/query", json={"query": "Iceberg compaction"}).status_code == 401
    # Authenticated public user query succeeds with 200
    ok_q = client.post(
        "/api/query",
        json={"query": "Iceberg Format V2 compaction 256MB"},
        headers={"Authorization": f"Bearer {isolated_config.public_token}"},
    )
    assert ok_q.status_code == 200
    assert ok_q.json()["results"][0]["document_id"] == "DOC-ARCH-001"

    # Non-admin DELETE rejected with 403; Admin DELETE propagates tombstone across Iceberg + Index!
    assert (
        client.delete(
            "/api/documents/DOC-ES-007",
            headers={"Authorization": f"Bearer {isolated_config.eng_lead_token}"},
        ).status_code
        == 403
    )
    del_ok = client.delete(
        "/api/documents/DOC-ES-007",
        headers={"Authorization": f"Bearer {isolated_config.admin_token}"},
    )
    assert del_ok.status_code == 200
    assert del_ok.json()["chunks_purged"] >= 1
    after_del = idx.query("Derecho al olvido eliminacion tombstone", PUBLIC_USER, top_k=5)
    assert all(h["document_id"] != "DOC-ES-007" for h in after_del["results"])
