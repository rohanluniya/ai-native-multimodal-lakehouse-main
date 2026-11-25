# Staff-Level Architecture & Interview Deep-Dive

This guide prepares a reviewer or engineer to walk through the hardest design tradeoffs in the AI-Native Multimodal Data Lakehouse & RAG Platform.

## 1. Why Not Put Everything Directly in Qdrant or OpenSearch?
- **Answer:** Vector and lexical search engines are optimized for low-latency top-k retrieval, not immutable time travel, schema evolution, lineage auditing, or multi-model re-embedding. By keeping raw binary metadata (`bronze.raw_assets`), parsed text & PII-redacted records (`silver.parsed_documents`), SCD Type 2 ACL history (`silver.document_acl_history`), and canonical chunks (`gold.document_chunks`) in Apache Iceberg V2, any search index corruption, schema change, or embedding upgrade recovers deterministically from Iceberg without re-crawling source systems.
- **Proof point:** In `tests/chaos/test_chaos_fault_injection.py`, wiping the serving index and rebuilding exclusively from Iceberg V2 completed in `22.08 ms` with exact top-1 retrieval parity.

## 2. Pre-Retrieval vs. Post-Retrieval ACL Enforcement
- **Trap:** Running top-k vector search first and filtering forbidden documents afterward leaks document existence via score/count side channels, wastes top-k slots (often returning 0 results to authorized users when restricted docs score higher), and fails if caches aren't tenant-scoped.
- **Implementation:** `multimodal_lakehouse/retrieval.py` evaluates `(tenant match) AND (NOT deny_match) AND (allow_match)` **before** BM25 or dense vector scoring, and keys the query cache on `(tenant_id, sorted(groups), query, mode, top_k)`. Hypothesis property tests fuzz arbitrary allow/deny intersections to confirm `deny-overrides-allow` holds unconditionally.

## 3. Handling Out-of-Order Revisions & GDPR Deletions
- **Monotonic revision guard:** If version `1` arrives late after version `2` has committed (e.g., CDC replay or network partition), the state reducer compares `revision_version` against active Iceberg state and rejects stale writes (`stale_out_of_order_revision_rejected` drill).
- **Tombstones vs. Iceberg Time Travel:** Operational serving purges deleted chunks immediately from active Gold views, serving indexes, and caches (`57.41 ms` measured). For full GDPR erasure from historical Parquet snapshots, Iceberg's `expire_snapshots` + orphan-file cleanup must run after tombstone commit, trading historical audit travel for regulatory compliance.

## 4. Why Did `fixed_size` Chunking Outperform `semantic` on Groundedness?
- **Measured finding (`results/benchmarks/retrieval_ablations.json`):** `fixed_size` achieved `1.0000` groundedness vs `0.9375` for `semantic` across BM25, hybrid, and hybrid+rerank.
- **Root cause:** Semantic splits cut strictly on markdown/section boundaries, occasionally separating a claim from its qualifying metric in the adjacent paragraph. Fixed-size overlapping windows preserved boundary overlap. In production, chunking strategy must be benchmarked against a labeled domain IR suite rather than assumed superior.

## 5. Blue/Green Re-Embedding Without Serving Downtime
- Instead of mutating vectors in place (which mixes incompatible vector spaces between `128d` and `256d` models during backfill), the platform tags every row in `gold.chunk_embeddings` with `model_version`, builds an isolated shadow collection for `embed-v2-charword-svd-256d`, verifies chunk count parity, and atomically flips the active alias pointer (`16.34 ms` on `smoke`, `67.01 ms` on `local`, `389.12 ms` on `stress`).
