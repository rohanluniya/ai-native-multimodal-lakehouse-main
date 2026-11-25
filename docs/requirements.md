# Functional & Non-Functional Requirements & SLOs

## Functional Requirements
- Ingest five binary/text modalities (`pdf`, `image`, `audio`, `markdown`, `structured`) across five simulated connectors (`file_drop`, `http_api`, `postgres_cdc`, `object_storage`, `kafka_cdc`).
- Preserve immutable raw content hashes, SCD Type 2 document revision & ACL history, parent-child chunk lineage, and dead-letter quarantine records.
- Support both `semantic` and `fixed_size` chunking strategies, dual embedding model versions with blue/green re-embedding migration, and hybrid BM25 + dense vector RRF retrieval with reranking and citations.
- Enforce pre-retrieval ACL filtering, PII redaction, prompt-injection defense, and full deletion tombstone propagation.

## Service-Level Objectives (SLOs)
- **Retrieval Latency (Local Single-Node):** `p95 < 25 ms` (measured `1.023 ms` local / `2.131 ms` stress).
- **ACL & Erasure Safety:** `0` unauthorized passage disclosures, `0` post-deletion disclosures, `0` unredacted SSN leaks.
- **GDPR Deletion Propagation SLA:** `< 60 seconds` across Iceberg, search indexes, and caches (measured `57.41 ms` locally).
- **Data Quality Gate:** `10/10` automated checks passing before promoting corpus updates.
