# Architecture Overview & Consistency Boundaries

## Dataflow & Failure Domains

1. **Immutable Source Ingestion (`bronze.raw_assets`):** Every binary document, image, WAV audio file, or structured JSON record is hashed (`SHA-256`) and persisted alongside immutable source URI, connector, event timestamp, and ACL lists. Duplicate `(document_id, version_num, content_sha256)` records are idempotently dropped.
2. **Quarantine-Protected Parsing DAG:** Binary assets pass through magic-byte validation, modality parsers (`pypdf`, `Pillow`, `wave`, JSON/Markdown), language detection, regex PII redaction, and prompt-injection flagging. Corrupt payloads route to `bronze.quarantine_dlq` without blocking valid traffic.
3. **SCD Type 2 Version & ACL Governance (`silver.document_versions_scd2`):** Tracks full document revision and permission history. Late/stale revisions with older `version_num` or older `event_time` are explicitly rejected so stale snapshots never overwrite current enterprise knowledge.
4. **Parent-Child Chunk Lineage (`silver.chunks_lineage` -> `gold.retrieval_corpus_active`):** Every chunk carries deterministic `(document_id, version_id, parent_chunk_id, chunk_id, char_start, char_end, chunk_sha256)` and inherits parent ACLs. Only current, non-deleted, PII-redacted chunks enter `gold.retrieval_corpus_active`.
5. **Derived Serving Indexes & Erasure Consistency:** BM25 and versioned vector indexes are strictly derived artifacts rebuildable from `gold.retrieval_corpus_active`. Deletion tombstones propagate synchronously in the API/pipeline across Iceberg Silver/Gold, BM25, all vector model versions, and query caches (`SLA < 60 s`; measured `57.41 ms` locally).
