"""Measured Benchmark Suite for Project 03: Load Profiles, Semantic vs Fixed Chunking, Retrieval Mode Ablations, and Iceberg V2 Compaction."""

from __future__ import annotations

import csv
import json
import platform
import resource
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from multimodal_lakehouse.config import LakehouseConfig
from multimodal_lakehouse.contracts import AvroContractRegistry
from multimodal_lakehouse.evaluation import PUBLIC_USER, run_evaluation_suite
from multimodal_lakehouse.generator import MultimodalCorpusGenerator
from multimodal_lakehouse.lakehouse import MultimodalLakehouse
from multimodal_lakehouse.retrieval import HybridRetrievalIndex
from multimodal_lakehouse.transport import LocalWalEventBus

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "results" / "benchmarks"
ASSETS_DIR = REPO_ROOT / "docs" / "assets"


def _rss_mb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / (1024 * 1024) if platform.system() == "Darwin" else r / 1024.0, 2)


def _mk_cfg(base: Path) -> LakehouseConfig:
    cfg = LakehouseConfig(
        env="bench",
        local_data_dir=base,
        raw_store_dir=base / "raw_assets",
        warehouse_dir=base / "warehouse",
        wal_dir=base / "wal",
        index_dir=base / "indexes",
        lineage_dir=base / "lineage",
        sqlite_catalog_path=base / "iceberg_catalog.db",
    )
    cfg.ensure_local_dirs()
    return cfg


def _pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return round(s[min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1))))], 3)


def run_load_profiles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sdir = REPO_ROOT / "schemas" / "avro"
    for prof in ("smoke", "local", "stress"):
        tmp = Path(tempfile.mkdtemp(prefix=f"mm_bench_{prof}_"))
        try:
            cfg = _mk_cfg(tmp)
            bus = LocalWalEventBus(cfg.wal_dir)
            reg = AvroContractRegistry(sdir)
            lh = MultimodalLakehouse(cfg)
            idx = HybridRetrievalIndex(cfg.index_dir)

            gen = MultimodalCorpusGenerator(cfg, bus, reg, profile=prof, seed=2026).run()
            proc = lh.process_from_bus(bus, reg, idx, chunk_strategy="semantic", run_migration_v2=True)
            dq = lh.run_data_quality(idx)

            # Measure 200 live uncached ACL-filtered hybrid queries
            queries = [
                "Iceberg Format V2 compaction 256MB retention",
                "Hybrid BM25 Qdrant Reciprocal Rank Fusion",
                "Kubernetes autoscaling FastAPI latency",
                "Kafka KRaft partition rebalancing",
                "GDPR right to be forgotten tombstone deletion",
            ]
            lats: list[float] = []
            for i in range(200):
                res = idx.query(queries[i % len(queries)], PUBLIC_USER, top_k=5, mode="hybrid_rerank", use_cache=False)
                lats.append(float(res["latency_ms"]))

            rows.append(
                {
                    "profile": prof,
                    "source_records": gen["total_source_records"],
                    "binary_bytes_generated": gen["counts"]["bytes_written"],
                    "active_chunks": proc["gold_active_chunks"],
                    "quarantined_dlq": proc["quarantined_dlq"],
                    "ingest_parse_index_s": proc["duration_seconds"],
                    "embed_v1_build_ms": proc["index_v1"]["build_duration_ms"],
                    "reembed_v2_ms": proc["migration_v2"]["migration_duration_ms"],
                    "query_p50_ms": _pct(lats, 50),
                    "query_p95_ms": _pct(lats, 95),
                    "query_p99_ms": _pct(lats, 99),
                    "dq_passed": dq["summary"]["passed"],
                    "dq_total": dq["summary"]["total"],
                    "peak_rss_mb": _rss_mb(),
                }
            )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return rows


def run_chunking_and_retrieval_ablations() -> dict[str, Any]:
    """Compare semantic vs fixed_size chunking and BM25 vs Vector vs Hybrid v1/v2 on local corpus."""
    sdir = REPO_ROOT / "schemas" / "avro"
    out: dict[str, Any] = {}
    for strategy in ("semantic", "fixed_size"):
        tmp = Path(tempfile.mkdtemp(prefix=f"mm_abl_{strategy}_"))
        try:
            cfg = _mk_cfg(tmp)
            bus = LocalWalEventBus(cfg.wal_dir)
            reg = AvroContractRegistry(sdir)
            lh = MultimodalLakehouse(cfg)
            idx = HybridRetrievalIndex(cfg.index_dir)
            MultimodalCorpusGenerator(cfg, bus, reg, profile="local", seed=2026).run()
            proc = lh.process_from_bus(bus, reg, idx, chunk_strategy=strategy, run_migration_v2=True)
            out[strategy] = {
                "active_chunks": proc["gold_active_chunks"],
                "embed_v2_ms": proc["migration_v2"]["migration_duration_ms"],
                "evaluations": run_evaluation_suite(idx),
            }
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return out


def run_iceberg_compaction_bench() -> dict[str, Any]:
    sdir = REPO_ROOT / "schemas" / "avro"
    tmp = Path(tempfile.mkdtemp(prefix="mm_comp_"))
    try:
        cfg = _mk_cfg(tmp)
        bus = LocalWalEventBus(cfg.wal_dir)
        reg = AvroContractRegistry(sdir)
        lh = MultimodalLakehouse(cfg)
        idx = HybridRetrievalIndex(cfg.index_dir)
        MultimodalCorpusGenerator(cfg, bus, reg, profile="stress", seed=2026).run()
        lh.process_from_bus(bus, reg, idx, run_migration_v2=False)

        # Create 6 uncompacted micro-batches in bronze.raw_assets
        base_rows = lh.read_arrow("bronze.raw_assets").to_pylist()
        for _ in range(5):
            lh.append_pylist("bronze.raw_assets", base_rows)

        before = lh.table_stats("bronze.raw_assets")
        t0 = time.perf_counter()
        c1 = lh.query_sql("SELECT modality, COUNT(*) AS cnt FROM bronze_raw_assets GROUP BY 1")
        scan_before_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        comp = lh.compact_table("bronze.raw_assets")
        t1 = time.perf_counter()
        c2 = lh.query_sql("SELECT modality, COUNT(*) AS cnt FROM bronze_raw_assets GROUP BY 1")
        scan_after_ms = round((time.perf_counter() - t1) * 1000.0, 2)
        assert sum(r["cnt"] for r in c1) == sum(r["cnt"] for r in c2)

        return {
            "files_before": before["data_file_count"],
            "files_after": comp["files_after"],
            "bytes_before": before["total_bytes"],
            "bytes_after": comp["bytes_after"],
            "storage_reduction_pct": round((1.0 - comp["bytes_after"] / max(1, before["total_bytes"])) * 100.0, 2),
            "scan_before_ms": scan_before_ms,
            "scan_after_ms": scan_after_ms,
            "compaction_ms": comp["duration_ms"],
            "records_preserved": comp["records_preserved"],
        }
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def plot_charts(load_rows: list[dict[str, Any]], abl: dict[str, Any]) -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "figure.facecolor": "#0b111b",
            "axes.facecolor": "#111a28",
            "axes.edgecolor": "#23334d",
            "axes.labelcolor": "#cbd5e1",
            "xtick.color": "#94a3b8",
            "ytick.color": "#94a3b8",
            "text.color": "#e2e8f0",
            "grid.color": "#1e2e45",
        }
    )

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 4.6), dpi=160)
    profs = [r["profile"] for r in load_rows]
    ax1.plot(profs, [r["query_p50_ms"] for r in load_rows], marker="o", linewidth=2.2, color="#34d399", label="p50 (ms)")
    ax1.plot(profs, [r["query_p95_ms"] for r in load_rows], marker="s", linewidth=2.2, color="#f59e0b", label="p95 (ms)")
    ax1.plot(profs, [r["query_p99_ms"] for r in load_rows], marker="^", linewidth=2.2, color="#f87171", label="p99 (ms)")
    ax1.set_title("ACL-Filtered Hybrid RAG Retrieval Latency (200 Uncached Queries)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Latency (ms)")
    ax1.grid(True, alpha=0.5)
    ax1.legend(facecolor="#0e1726", edgecolor="#23334d")

    sem_evals = abl["semantic"]["evaluations"]
    labels = [f"{e['mode']}\n({e['model_version'].split('-')[1]})" for e in sem_evals]
    x = list(range(len(labels)))
    w = 0.28
    ax2.bar([i - w for i in x], [e["recall_at_5"] for e in sem_evals], width=w, color="#34d399", label="Recall@5")
    ax2.bar(x, [e["mrr_at_5"] for e in sem_evals], width=w, color="#2dd4bf", label="MRR@5")
    ax2.bar([i + w for i in x], [e["groundedness"] for e in sem_evals], width=w, color="#60a5fa", label="Groundedness")
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=8.5)
    ax2.set_ylim(0, 1.15)
    ax2.set_title("Retrieval Quality Across Modes & Re-Embedded Model Versions", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Score (0.0 – 1.0)")
    ax2.grid(axis="y", alpha=0.5)
    ax2.legend(facecolor="#0e1726", edgecolor="#23334d")

    fig.tight_layout()
    fig.savefig(ASSETS_DIR / "benchmark_rag_latency_and_quality.png", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    load_rows = run_load_profiles()
    abl = run_chunking_and_retrieval_ablations()
    comp = run_iceberg_compaction_bench()
    plot_charts(load_rows, abl)

    meta = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "execution_profile": "local_native",
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "python_version": platform.python_version(),
    }
    (RESULTS_DIR / "load_profiles.json").write_text(json.dumps({"metadata": meta, "profiles": load_rows}, indent=2), encoding="utf-8")
    with (RESULTS_DIR / "load_profiles.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(load_rows[0].keys()))
        w.writeheader()
        w.writerows(load_rows)
    (RESULTS_DIR / "retrieval_ablations.json").write_text(json.dumps({"metadata": meta, "ablations": abl}, indent=2), encoding="utf-8")
    (RESULTS_DIR / "iceberg_compaction.json").write_text(json.dumps({"metadata": meta, "compaction": comp}, indent=2), encoding="utf-8")
    print(json.dumps({"load_profiles": load_rows, "iceberg_compaction": comp, "chunking_counts": {k: v["active_chunks"] for k, v in abl.items()}}, indent=2))


if __name__ == "__main__":
    main()
