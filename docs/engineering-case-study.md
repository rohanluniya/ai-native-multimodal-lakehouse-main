# Engineering Case Study: Treating Serving Search Indexes as Disposable Views Over an Apache Iceberg V2 Lakehouse

## Problem Statement

Enterprise Retrieval-Augmented Generation (RAG) stacks routinely fail in production for four non-ML reasons:
1. **Unstructured governance drift:** PDFs, diagrams, audio transcripts, and Markdown wikis are parsed directly into a standalone vector database without immutable lineage or raw binary checksums.
2. **ACL leakage across retrieval boundaries:** Applying access control *after* top-k vector search causes empty result sets when top matches belong to restricted tenants, or leaks restricted snippets into reranker prompts and caches.
3. **Right-to-be-forgotten gaps:** Deleting a document from an upstream wiki leaves stale chunks alive in vector indexes, BM25 indexes, and semantic caches.
4. **Destructive embedding upgrades:** Swapping embedding models requires wiping and re-ingesting raw documents from external SaaS APIs, causing serving downtime.

## Architectural Solution

This platform makes **Apache Iceberg V2** (`bronze -> silver -> gold -> quality`) the sole authoritative system of record and treats all serving indexes (vector + BM25 + query cache) as **disposable, rebuildable materialized views**:

- **Zero-trust pre-retrieval ACL filtering:** Every chunk carries `tenant_id`, `allow_groups`, and `deny_groups` synchronized from SCD Type 2 `silver.document_acl_history`. Retrieval evaluates `deny-overrides-allow` *before* scoring BM25 or cosine similarity.
- **Deterministic multimodal parsing & quarantine:** `pypdf`, `Pillow`, and `wave` inspect real binaries; corrupt media, schema-invalid Avro frames, and prompt-injection payloads route to `bronze.quarantine_dlq` without halting healthy ingestion.
- **Tombstone propagation & cache purging:** Deleting a document appends an immutable record to `silver.deletion_tombstones`, removes active chunks from the serving index, and invalidates cached responses referencing that `document_id`.
- **Blue/green re-embedding:** Re-embedding reads `gold.document_chunks` directly from Iceberg, builds the shadow index for `embed-v2-charword-svd-256d`, validates readiness, and atomically swaps the serving alias.

## Measured Outcomes (`local_native` Profile)

- **Index rebuild resilience:** Wiping the serving index and rebuilding from Iceberg restored all `36` chunks in **`22.08 ms`** with exact top-1 parity (`results/chaos/chaos_results.json`).
- **Zero security/compliance leakage:** **`0`** ACL leaks, **`0`** prompt-injection leaks, and **`0`** deleted-document leaks across all 12 evaluation queries, 7 chaos drills, and Hypothesis property-based fuzzing.
- **Sub-3ms tail latency at stress scale:** Hybrid BM25 + dense vector RRF + reranking achieved **`1.023 ms`** p95 on `local` (122 chunks) and **`2.131 ms`** p95 on `stress` (590 chunks).
- **Compaction payoff:** Compacting 6 fragmented Iceberg Parquet files into 1 reduced storage bytes by **`-74.64%`** (`3.94×` smaller) and scan latency by **`-27.8%`** (`1.39×` faster).
- **Surprising ablation insight:** Fixed-size sliding windows beat semantic heading chunking on citation groundedness (`1.0000` vs `0.9375`) on this corpus because cross-paragraph context stayed intact; documenting this honestly prevents blind adoption of complex chunkers without domain evaluation.
