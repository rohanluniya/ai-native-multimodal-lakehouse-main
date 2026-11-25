# Incident Simulation Report: INC-001 Chaos Drill

- **Severity:** Planned Game Day (Automated Chaos Suite)
- **Target System:** AI-Native Multimodal Data Lakehouse & RAG Platform (`local_native` profile)
- **Artifact Source:** `results/chaos/chaos_results.json` and `tests/chaos/test_chaos_fault_injection.py`
- **Outcome:** **7 / 7 scenarios PASSED (`0` failures)**

## Executive Summary

During the automated reliability game day, seven failure modes spanning ingestion transport, corrupt multimodal binaries, serving index loss, out-of-order document revisions, adversarial ACL/prompt-injection payloads, GDPR right-to-be-forgotten tombstones, and blue/green embedding upgrades were injected sequentially. Every fault remained contained within its blast radius, zero unauthorized or deleted chunks leaked into retrieval results, and full serving parity recovered automatically from Apache Iceberg V2 Gold tables.

## Scenario Timeline & Measured Evidence

| # | Scenario | Injected Fault | Containment & Recovery Behavior | Measured Result |
| --- | --- | --- | --- | --- |
| 1 | `broker_outage_and_recovery` | Simulated Kafka broker outage during publish | Durable file-backed spool persisted unacked frames and resumed cleanly at offset `0` on reconnect | **PASS** (`1.84 ms`) |
| 2 | `corrupt_media_and_poison_avro_quarantine` | Truncated PDF/PNG/WAV binaries + schema-invalid Avro event | Parser & contract validators routed `5` bad records to `bronze.quarantine_dlq` while `36` valid chunks committed normally | **PASS** (`398.46 ms`) |
| 3 | `index_loss_and_rebuild_from_iceberg` | Complete wipe of in-memory/serving search index | Rebuilt vector + BM25 index exclusively from Iceberg V2 `gold.document_chunks` with exact top-1 retrieval parity | **PASS** (`22.08 ms` rebuild; `197.98 ms` total) |
| 4 | `stale_out_of_order_revision_rejected` | Arriving stale `version=1` event after `version=2` was active | Monotonic version guard rejected `1` stale overwrite and preserved `active_version=2` | **PASS** (`186.18 ms`) |
| 5 | `conflicting_acl_and_prompt_injection_shield` | Document with both `allow` and `deny` for same group + hidden prompt-injection instructions | Pre-retrieval ACL evaluator enforced deny-overrides-allow (`0` leaks); sanitizer blocked `2` injection chunks | **PASS** (`176.35 ms`) |
| 6 | `deletion_tombstone_cache_invalidation` | GDPR deletion tombstone for an indexed, cached document | Tombstone propagated across `silver.deletion_tombstones`, active chunk view, search index, and query cache (`2` chunks purged) | **PASS** (`57.41 ms` purge; `238.02 ms` total) |
| 7 | `blue_green_reembed_migration` | Live upgrade from `embed-v1-tfidf-svd-128d` to `embed-v2-charword-svd-256d` | Shadow index built for all `36` chunks and swapped atomically without rejecting queries | **PASS** (`16.34 ms` migration; `278.49 ms` total) |

## Follow-Up Actions
- Keep `tests/chaos/test_chaos_fault_injection.py` gated in `.github/workflows/ci.yml` on every pull request.
- In distributed cluster deployments (`docker-compose.yml`), pair index rebuild tests with Qdrant snapshot verification and OpenSearch alias rollback validation.
