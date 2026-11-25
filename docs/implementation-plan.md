# Implementation Plan & Delivery Milestones

This document maps the architecture of the AI-Native Multimodal Data Lakehouse & RAG Platform onto concrete engineering phases and verification gates.

## Phase 1: Contracts, Multimodal Generation & Medallion Storage
- **Deliverables:**
  - Avro schemas (`schemas/avro/source_asset_event.avsc`, `deletion_tombstone_event.avsc`, `dlq_event.avsc`, and backward-compatible `v2/source_asset_event.avsc`).
  - Deterministic multimodal binary generator (`multimodal_lakehouse/generator.py`) synthesizing real PDF 1.4 documents, Pillow PNG diagrams with OCR text, PCM WAV audio files, Markdown, and JSON records alongside controlled failure modes (stale revisions, corrupt binaries, conflicting ACLs, PII, prompt injections, multilingual text, duplicates, and GDPR tombstones).
  - PyIceberg V2 warehouse (`multimodal_lakehouse/lakehouse.py`) managing 8 medallion/governance tables (`bronze.raw_assets`, `bronze.quarantine_dlq`, `silver.parsed_documents`, `silver.document_acl_history`, `silver.deletion_tombstones`, `gold.document_chunks`, `gold.chunk_embeddings`, `quality.dq_results`) with SQLite catalog persistence and DuckDB analytical reads.
- **Gate:** Avro v1/v2 compatibility unit tests and 10/10 Iceberg DQ assertions pass.

## Phase 2: Parsing, Chunking, PII Redaction & Security Controls
- **Deliverables:**
  - Modality-specific parser (`multimodal_lakehouse/parsing.py`) with `pypdf`, `Pillow`, and `wave` inspection, corrupt-byte quarantine routing, SSN/email regex redaction, and prompt-injection pattern detection.
  - Pluggable chunking engine (`multimodal_lakehouse/chunking.py`) supporting `semantic` and `fixed_size` policies with deterministic `chunk_id`, parent-child document lineage, token counts, and source byte/page provenance.
- **Gate:** Hypothesis property-based tests prove SSN redaction never leaks 9-digit patterns and malformed binaries route to `bronze.quarantine_dlq` without blocking valid ingestion.

## Phase 3: ACL-Enforced Hybrid Retrieval, Blue/Green Embeddings & Evaluation
- **Deliverables:**
  - Local hybrid retrieval engine (`multimodal_lakehouse/retrieval.py`) combining strict pre-retrieval ACL filtering (`deny-overrides-allow`), BM25 lexical scoring, deterministic zero-download dense embeddings (`embed-v1-tfidf-svd-128d` and `embed-v2-charword-svd-256d`), Reciprocal Rank Fusion (RRF), lexical-overlap reranking, cache invalidation, and zero-downtime blue/green model migration.
  - Retrieval & security evaluation harness (`multimodal_lakehouse/evaluation.py`) measuring Recall@1, Recall@5, MRR@5, bounded document-level nDCG@5, citation groundedness, ACL leakage, prompt-injection leakage, and deleted-document leakage across 12 fixtures.
- **Gate:** Hypothesis property tests confirm `0` ACL leaks under arbitrary randomized allow/deny role combinations, and retrieval ablations show `0` ACL, injection, or deleted-document leaks.

## Phase 4: Distributed Engines, Control Tower API/UI & Portability
- **Deliverables:**
  - PySpark batch re-embedding job (`processing/spark/multimodal_lakehouse_job.py`), Airflow 3.4 DAG (`processing/airflow/dags/multimodal_dags.py`), PostGIS outbox & Debezium CDC configuration (`ingestion/cdc/`), and Trino analytical SQL (`sql/trino/multimodal_analytics.sql`).
  - FastAPI RAG & Governance API (`apps/rag-api/main.py`) with RBAC header enforcement, Prometheus metrics (`/metrics`), and live interactive Control Tower UI (`apps/rag-dashboard/`).
  - Portable deployment artifacts: `Dockerfile.standalone`, `docker-compose.standalone.yml`, zero-Docker `scripts/run_portable.sh`, multi-profile `docker-compose.yml` (all versions pinned), Kubernetes manifests, and GitHub Actions CI.
- **Gate:** Live server verified on `http://127.0.0.1:8020`, real headless Chrome screenshots inspected, and 7/7 chaos drills + 3-profile benchmarks persisted to `results/`.
