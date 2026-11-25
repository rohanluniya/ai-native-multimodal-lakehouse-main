# Live 5-Minute Walkthrough & Demo Script

Use this script to demonstrate the AI-Native Multimodal Data Lakehouse & RAG Platform end-to-end on any workstation.

## 1. Boot the Platform (Zero Docker Required)
```bash
./scripts/run_portable.sh
```
This prepares `.venv`, ingests the `local` multimodal corpus (PDF, PNG, WAV, Markdown, JSON) into Apache Iceberg V2, runs all 10 data quality checks, and serves the interactive Control Tower at `http://127.0.0.1:8020`.

## 2. Inspect Medallion Ingestion & Governance in the UI
Open `http://127.0.0.1:8020` in a browser:
1. **Top KPI Strip:** Verify active chunks (`122`), quarantined DLQ assets (`7` corrupt binaries/poison payloads isolated without halting ingestion), DQ pass count (`10/10`), and active embedding model (`embed-v1-tfidf-svd-128d`).
2. **Governance & Quarantine Panel:** Inspect `bronze.quarantine_dlq` entries (`CORRUPT_PDF`, `CORRUPT_IMAGE`, `CORRUPT_AUDIO`, `PROMPT_INJECTION_DETECTED`) and confirm all 10 Iceberg DQ checks show `PASS`.

## 3. Demonstrate Pre-Retrieval ACL Isolation
In the **Interactive RAG Console** (or via `curl`):
1. Query `"executive M&A acquisition budget"` as `tenant_acme` with group `engineering` (not authorized for executive docs):
   - Observe **0 restricted chunks returned** (`acl_filtered` enforces `deny-overrides-allow` before scoring).
2. Switch group to `executive`:
   - Observe grounded citations returned with exact `document_id`, `chunk_id`, `modality`, and `[REDACTED_SSN]` PII scrubbing intact.

## 4. Trigger Live Blue/Green Re-Embedding
Click **Migrate to `embed-v2` (Blue/Green)** (or `POST /v1/admin/reembed`):
- Re-embeds all active chunks from `gold.document_chunks` into `embed-v2-charword-svd-256d` and atomically flips the active alias without dropping requests.

## 5. Prove GDPR Tombstone Propagation & Cache Invalidation
Click **Issue GDPR Tombstone** for an indexed document (or `POST /v1/admin/tombstone`):
- Appends to `silver.deletion_tombstones`, purges active chunks from the serving index, clears cached answers referencing that document, and immediately returns `0` matches on repeat queries.

## 6. Run Verification & Benchmarks
```bash
.venv/bin/pytest -v
.venv/bin/python benchmarks/run_benchmarks.py
```
Inspect persisted outputs in `results/benchmarks/`, `results/chaos/chaos_results.json`, and `docs/assets/benchmark_rag_latency_and_quality.png`.
