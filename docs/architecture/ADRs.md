# Architecture Decision Records (ADRs)

## ADR-001: Pre-Retrieval ACL Filtering with Deny-Overrides-Allow
- **Decision:** Evaluate `(security_tier, acl_allow_groups, acl_deny_groups)` strictly *before* BM25 or dense vector scoring; deny always overrides allow for non-admin principals.
- **Rationale:** Post-retrieval filtering leaks score distributions, causes variable top-k exhaustion, and risks prompt-stuffing sensitive passages if a downstream bug drops a filter. Proven via Hypothesis property testing (`test_property_deny_always_overrides_allow_for_non_admin`).

## ADR-002: Zero-Download Deterministic Self-Hosted Embeddings for Reproducible CI & Governance
- **Decision:** Implement dual versioned TF-IDF + TruncatedSVD embeddings (`embed-v1-tfidf-svd-128d` and `embed-v2-charword-svd-256d`) in-process via `scikit-learn`/`numpy`, keeping external Hugging Face models strictly optional and subject to pre-download license review.
- **Rationale:** Guarantees deterministic CI reproducibility, sub-millisecond local retrieval, zero unlicensed model weight risk, and real blue/green re-embedding migration benchmarks without requiring GPUs.

## ADR-003: Apache Iceberg Format V2 + Lakekeeper REST + RustFS as Authoritative Corpus Store
- **Decision:** Store immutable raw assets, SCD2 document versions, chunk lineage, tombstones, and evaluation logs in Apache Iceberg V2 backed by Lakekeeper v0.15.1 and RustFS 1.0.6 (replacing archived MinIO CE).
- **Rationale:** Vector databases (Qdrant) and search engines (OpenSearch) are derived indexes, not transactional audit ledgers. Iceberg V2 enables point-in-time replay, compaction (`-74.64%` storage), and deterministic index rebuilds after disk loss (`22.08 ms` verified).

## ADR-004: Redact PII Prior to Chunking & Embedding
- **Decision:** Run regex/pattern PII redaction (`SSN`, `EMAIL`, `PHONE`, `PAYMENT_CARD`) during parsing so raw PII never enters `silver.chunks_lineage`, `gold.retrieval_corpus_active`, vector spaces, or RAG citations.
- **Rationale:** Once PII is encoded into dense vectors or LLM context caches, surgical removal is far harder than upstream prevention. Verified continuously by DQ check `zero_raw_ssn_in_active_corpus`.
