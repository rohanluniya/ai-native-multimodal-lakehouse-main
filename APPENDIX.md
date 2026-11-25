# Technical Appendix — Chunking Ablations, Retrieval Tradeoffs & Acceptance Matrix

## 1. Semantic vs. Fixed-Size Chunking Ablation (`local` profile, 88 source records)

Raw evidence: [`results/benchmarks/retrieval_ablations.json`](results/benchmarks/retrieval_ablations.json).

| Strategy | Active Chunks | `embed-v2` Build (ms) | `hybrid_rerank` Recall@5 | `hybrid_rerank` MRR@5 | `hybrid_rerank` Groundedness | `hybrid_rerank` p95 (ms) | Tradeoff |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `semantic` (heading/paragraph) | 122 | 68.05 | 1.00 | 1.00 | 0.9375 | 0.768 | Clean section boundaries & coherent citations; slightly splits multi-heading keyword spans across two child chunks |
| `fixed_size` (45-word / 10-word overlap) | 121 | 76.88 | 1.00 | 1.00 | **1.0000** | 0.905 | Overlapping sliding windows capture cross-heading phrases (`+6.25 pp` groundedness vs. semantic) at `+17.8%` higher p95 latency and `+13.0%` slower v2 index build |

## 2. Retrieval Mode & Embedding Model Cost-Quality Tradeoff

- Moving from `bm25` (`0.157 ms` p95) to `hybrid` RRF v1 (`0.412 ms` p95, `2.62×` slower than BM25) improved semantic groundedness from `0.875` to `0.9375` (`+6.25 pp`).
- Upgrading from word unigrams (`embed-v1-tfidf-svd-128d`, `8.56 ms` build) to subword char_wb 3–5 n-grams (`embed-v2-charword-svd-256d`, `67.01 ms` build, `7.83×` heavier build cost) adds morphological robustness for multilingual queries (`es`, `fr`, `de`) while staying under `1.05 ms` p95 query latency locally.
- **Bug Caught During Verification:** Initial nDCG@5 calculation summed duplicate hits from two chunks belonging to the same relevant document, producing `1.2366 > 1.0`. Deduplicating `document_id` during DCG accumulation bounded `nDCG@5` strictly in `[0.0, 1.0]` (`1.0000`).

## 3. Mandatory Acceptance Test Matrix (1–15)

| # | Mandatory Acceptance Criterion | Verification Artifact | Status |
| --- | --- | --- | --- |
| 1 | Fresh-clone setup starts core stack & verifies `/health` | `scripts/run_portable.sh`, `Dockerfile.standalone`, `GET /health` (`200 OK`) | PASS |
| 2 | Deterministic seeded generation yields stable IDs & outcomes | `MultimodalCorpusGenerator` (`seed=2026`), UUIDv5 deterministic IDs | PASS |
| 3 | Real ingestion persists raw binary assets & source metadata with replay | `LocalWalEventBus`, `bronze.raw_assets`, `.local_data/raw_assets/` | PASS |
| 4 | Core domain transformation yields correct results for known fixtures | `test_e2e_multimodal_pipeline_acl_revisions_erasure_and_api` | PASS |
| 5 | Advanced domain features operational E2E (OCR, Audio, ACL, RRF, Re-Embed, GDPR Erasure) | `test_multimodal_e2e.py`, `apps/rag-api/main.py` | PASS |
| 6 | Duplicates, late stale revisions & corrections handled without corruption | `test_chaos_04_stale_out_of_order_revision_rejected` (`8` dups, `1` stale v1 rejected) | PASS |
| 7 | Crash/disk loss recovers according to tested guarantee | `test_chaos_03_index_loss_and_rebuild_from_iceberg_gold` (`22.08 ms`, exact parity) | PASS |
| 8 | Replay/backfill & re-embedding produce reconcilable results | `test_chaos_07_blue_green_reembed_migration_zero_leak`, `10/10` DQ parity | PASS |
| 9 | Primary RAG API & Control Tower return real computed outputs with freshness | Live API on `:8020`, `docs/assets/rag_control_tower_full.png` | PASS |
| 10 | DQ checks catch invalid inputs & retain auditable quarantine records | `bronze.quarantine_dlq` (`7` records), `quality.dq_check_results` (`10/10 PASS`) | PASS |
| 11 | Observability emits live structured JSON logs, Prometheus metrics, OTel spans & OpenLineage | `/metrics`, `openlineage_events.jsonl`, `observability.py` | PASS |
| 12 | Access control rejects unauthorized requests & enforces deny-overrides-allow | `401` unauth `/api/query`, `403` non-admin `DELETE`, Hypothesis property tests | PASS |
| 13 | Controlled failure produces incident report with observed recovery timings | `docs/incidents/INC-001-chaos-drill.md`, `results/chaos/chaos_results.json` (`7/7 PASS`) | PASS |
| 14 | Benchmark records hardware, volume, commands, p50/p95/p99 & raw outputs | `results/benchmarks/*.{json,csv}`, `docs/benchmarks/benchmark-report.md` | PASS |
| 15 | CI, lint, mypy, license audit & SBOM pass cleanly | `.github/workflows/ci.yml`, `results/security/{license_audit,sbom.cdx}.json` | PASS |
