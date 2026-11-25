# Component Compatibility & License Matrix

| Component | Pinned Release | License | Multi-Arch | Role |
| --- | --- | --- | --- | --- |
| Apache Kafka (KRaft) | `apache/kafka:4.3.2` | Apache-2.0 | `amd64`/`arm64` | Durable source events, ACL updates, tombstones, DLQ |
| Qdrant Vector DB | `qdrant/qdrant:v1.20.2-unprivileged` | Apache-2.0 | `amd64`/`arm64` | Dense vector ANN index with metadata/ACL payload filters |
| OpenSearch | `opensearchproject/opensearch:3.9.0` | Apache-2.0 | `amd64`/`arm64` | Distributed BM25 lexical search engine |
| Apache Tika Full | `apache/tika:3.4.0.0-full` | Apache-2.0 | `amd64`/`arm64` | Document, PDF, Tesseract OCR, and media metadata parser |
| RustFS Object Store | `rustfs/rustfs:1.0.6` | Apache-2.0 | `amd64`/`arm64` | S3-compatible object store (replaces archived MinIO CE) |
| Lakekeeper Catalog | `quay.io/lakekeeper/catalog:v0.15.1` | Apache-2.0 | `amd64`/`arm64` | Apache Iceberg V2 REST catalog |
| PostgreSQL + PostGIS | `imresamu/postgis:17-3.6.1-alpine3.23` | PostgreSQL/GPL-2.0 | `amd64`/`arm64` | Operational document/ACL metadata store |
| Debezium Connect | `quay.io/debezium/connect:3.7.1.Final` | Apache-2.0 | `amd64`/`arm64` | PostgreSQL logical replication CDC |
| Apache Spark / Trino / Airflow | `4.1.4` / `484` / `slim-3.4.0` | Apache-2.0 | `amd64`/`arm64` | Batch curation, federated SQL, workflow orchestration |
| Prometheus / Grafana / Loki / Marquez | `v3.16.0` / `13.0.14` / `3.6.22` / `0.51.1` | Apache-2.0 / AGPL-3.0 | `amd64`/`arm64` (`marquez`: `amd64`) | Metrics, dashboards, logs, OpenLineage v2 |
| Python Local Runtime | Python `3.12.8`, `pyiceberg==0.12.0`, `duckdb==2.0.0`, `scikit-learn==1.9.1`, `pypdf==6.21.4`, `pillow==12.3.0`, `fastapi==0.163.2` | Permissive OSS | Native `arm64`/`amd64` | 75 packages audited, 0 copyleft/proprietary flags |
