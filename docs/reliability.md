# Reliability, SLOs & Incident Runbooks

This document defines the service-level objectives (SLOs), fault-isolation boundaries, and operational runbooks referenced by `infra/observability/alerts.yml`.

## Service-Level Objectives

| Signal | Objective | Measured (`local` Profile) | Measurement Source |
| --- | --- | --- | --- |
| Retrieval p95 latency (hybrid + rerank) | `< 100 ms` | `1.023 ms` (`stress`: `2.131 ms`) | `results/benchmarks/load_profiles.json` |
| ACL isolation enforcement | `0` unauthorized chunks returned | `0` across all 12 eval queries & Hypothesis fuzzing | `results/benchmarks/retrieval_ablations.json` |
| Deleted-document tombstone purge | `0` deleted chunks retrievable post-tombstone | `0` leaks; `57.41 ms` propagation | `results/chaos/chaos_results.json` |
| Data quality gate pass rate | `10 / 10` checks passing | `10 / 10` on `smoke`, `local`, `stress` | `results/benchmarks/load_profiles.json` |
| Index rebuild from Iceberg | Full parity recovery without re-ingesting raw sources | `22.08 ms` for 36 chunks, exact top-1 parity | `results/chaos/chaos_results.json` |

## Chaos Verification Summary

All 7 automated resilience drills in `tests/chaos/test_chaos_fault_injection.py` passed (`results/chaos/chaos_results.json`):
1. `broker_outage_and_recovery` (`1.84 ms`): Local Kafka spool survives simulated broker outage and resumes cleanly from offset `0`.
2. `corrupt_media_and_poison_avro_quarantine` (`398.46 ms`): Corrupt PDF/PNG/WAV files and poison payloads route to `bronze.quarantine_dlq` (`5` quarantined) while `36` valid chunks ingest without interruption.
3. `index_loss_and_rebuild_from_iceberg` (`197.98 ms`): Deleting the active serving index and rebuilding exclusively from Iceberg V2 `gold.document_chunks` restores all `36` chunks in `22.08 ms` with exact top-1 retrieval parity.
4. `stale_out_of_order_revision_rejected` (`186.18 ms`): Late-arriving version `1` update after version `2` is rejected (`stale_rejected=1`), keeping `active_version=2`.
5. `conflicting_acl_and_prompt_injection_shield` (`176.35 ms`): Explicit deny overrides allow (`acl_leaks=0`) and prompt-injection payloads are blocked (`injections_blocked=2`).
6. `deletion_tombstone_cache_invalidation` (`238.02 ms`): GDPR tombstone purges `2` chunks from Iceberg active view, vector/lexical index, and query cache in `57.41 ms`.
7. `blue_green_reembed_migration` (`278.49 ms`): Re-embedding all `36` chunks from `embed-v1-tfidf-svd-128d` to `embed-v2-charword-svd-256d` completes in `16.34 ms` with atomic alias swap and zero downtime.

---

## Alert Runbooks

### Quarantine Triage {#quarantine-triage}
**Triggered by:** `MultimodalQuarantineSpike` (`increase(multimodal_quarantined_assets_total[15m]) > 20`).
1. Query `GET /v1/governance/quarantine` or inspect Iceberg `bronze.quarantine_dlq` via DuckDB (`SELECT error_code, COUNT(*) FROM bronze_quarantine_dlq GROUP BY 1`).
2. Distinguish **upstream media corruption** (`CORRUPT_PDF`, `CORRUPT_IMAGE`, `CORRUPT_AUDIO`) from **schema contract breaks** (`AVRO_SCHEMA_MISMATCH`) or **security quarantines** (`PROMPT_INJECTION_DETECTED`).
3. For schema mismatches, verify the producer against `schemas/avro/v2/source_asset_event.avsc` and Apicurio Registry compatibility rules.
4. Do **not** blindly replay quarantined rows; fix upstream producers or parser tolerances, then replay only validated `asset_id`s.

### DQ Failure {#dq-failure}
**Triggered by:** `MultimodalDataQualityFailure` (`increase(multimodal_dq_checks_total{status="FAIL"}[15m]) > 0`).
1. Query `GET /v1/governance/dq` or read `quality.dq_results` to identify the exact failed check (`no_raw_pii_ssn_in_chunks`, `deleted_docs_purged_from_index`, `acl_coverage_complete`, `chunk_lineage_integrity`, etc.).
2. If `no_raw_pii_ssn_in_chunks` or `deleted_docs_purged_from_index` fails, immediately pause external RAG traffic or trigger cache flush + tombstone purge (`POST /v1/admin/tombstone`).
3. If `index_iceberg_parity` fails, run index rebuild from Iceberg Gold tables (`multimodal_lakehouse.retrieval` rebuild path) and verify row counts match `gold.document_chunks`.
4. Confirm all 10 DQ checks return `PASS` before clearing the incident.

### Retrieval Latency SLO {#retrieval-latency-slo}
**Triggered by:** `MultimodalRetrievalLatencyHigh` (`p95 > 100ms` for 3m).
1. Inspect Prometheus histograms and `/v1/metrics/summary` to separate retrieval compute (`bm25`, `vector`, `rerank`) from cache misses or concurrent re-embedding load.
2. Verify whether a blue/green re-embedding job (`embed-v2-charword-svd-256d`) is contending for CPU/memory on the serving host; throttle background workers if so.
3. Check Iceberg file fragmentation (`bronze.raw_assets`, `gold.document_chunks`) and run compaction if uncompacted file counts have spiked.
4. Confirm query cache hit ratio recovered and p95 latency returns below `100 ms`.
