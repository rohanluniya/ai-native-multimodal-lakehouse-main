# Capacity Model & Sizing

This document pairs **measured single-node `local_native` resource footprints** (from `results/benchmarks/load_profiles.json` and `results/benchmarks/iceberg_compaction.json`) with **analytic scale-out sizing calculations** for larger multimodal corpora.

## Measured Local Resource Footprint

| Profile | Source Assets | Raw Binary Bytes | Active Chunks | Quarantined | E2E Pipeline Time | Query p95 | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `smoke` | 33 | 101,960 B | 36 | 5 | 0.133 s | 0.384 ms | 345.09 MB |
| `local` | 88 | 363,181 B | 122 | 7 | 0.208 s | 1.023 ms | 412.28 MB |
| `stress` | 372 | 1,799,795 B | 590 | 14 | 0.675 s | 2.131 ms | 495.23 MB |

**Observed scaling ratios (measured `stress` vs `local`):**
- Asset count increases `4.23×` (`88 -> 372`) and active chunks increase `4.84×` (`122 -> 590`).
- End-to-end ingestion/parse/chunk/index wall time increases `3.25×` (`0.208 s -> 0.675 s`), yielding **550.9 assets/s** and **873.7 chunks/s** including real PDF/PNG/WAV extraction and Iceberg V2 commits.
- Peak RSS grows by `+82.95 MB` (`412.28 MB -> 495.23 MB`, `+20.1%`) across a `4.84×` chunk expansion, because the fixed Python/PyArrow/DuckDB baseline dominates at sub-1k chunks.

## Analytic Scale-Out Model (Unmeasured Projections)

The rows below are **engineering sizing formulas** (not measured benchmarks) for running the distributed `docker-compose.yml` / Kubernetes stack on multi-million chunk corpora:

### Storage Footprint (Per 1,000,000 Active Chunks)

| Layer | Formula | Estimate at 1M Chunks |
| --- | --- | ---: |
| Raw Object Store (S3 / RustFS) | ~500 KB avg raw PDF/image/audio per 10 chunks -> 100k objects × 500 KB | ~50 GB |
| Iceberg V2 Silver + Gold Parquet (Zstd) | ~1.5 KB text + metadata + 256d float32 vector (~1 KB) compressed ~3× | ~0.85 GB |
| Qdrant Vector Index (256d float32 + HNSW graph `m=16`) | `1M × (256×4 + 16×8×2)` bytes ≈ `1.28 GB` RAM + payload index | ~1.8 GB RAM |
| OpenSearch BM25 Inverted Index (1 replica) | ~1.2× raw chunk text (~800 B/chunk) × 2 copies | ~1.9 GB disk |
| Query Cache (Valkey / in-memory) | 100k cached responses × 4 KB | ~400 MB RAM |

### Re-Embedding Compute Budget

Measured local `embed-v2-charword-svd-256d` throughput on `stress` (590 chunks in `389.12 ms`) equals **1,516 chunks/s** on a single CPU process.
- At **1,516 chunks/s per CPU worker**, a 10M-chunk blue/green re-embedding migration completes in ~110 minutes on 1 CPU worker or **~7 minutes across 16 parallel Spark/Ray CPU workers**.
- If replaced by a dense GPU transformer encoder (e.g., 512-token batches at 2,000 chunks/s per GPU), the same 10M-chunk migration takes ~83 GPU-minutes (~10.4 minutes on 8 GPUs) while keeping zero serving downtime via blue/green collection aliases.

## Iceberg Small-File & Compaction Budget

Measured compaction on `bronze.raw_assets` (`results/benchmarks/iceberg_compaction.json`):
- Rewriting 6 small Parquet files (`185,706 B`) into 1 compacted file (`47,098 B`) achieved **`-74.64%` byte reduction (`3.94×` smaller)** and **`-27.8%` scan latency (`15.09 ms -> 10.89 ms`, `1.39×` faster)** in `29.2 ms`.
- Operational target: trigger Airflow Iceberg compaction whenever a partition exceeds 32 small files or 128 MB of uncompacted appends, and expire snapshots older than 7 days (after GDPR tombstone propagation is verified).
