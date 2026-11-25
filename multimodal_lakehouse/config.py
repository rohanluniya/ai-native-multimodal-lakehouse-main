"""Typed environment-driven configuration for the AI-Native Multimodal Lakehouse & RAG Platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw is not None and raw.strip() else default


@dataclass(frozen=True)
class LakehouseConfig:
    env: str = field(default_factory=lambda: _env("LAKEHOUSE_ENV", "dev"))
    execution_profile: str = field(
        default_factory=lambda: _env("LAKEHOUSE_EXECUTION_PROFILE", "local_native")
    )
    transport_mode: str = field(
        default_factory=lambda: _env("LAKEHOUSE_TRANSPORT_MODE", "local_wal")
    )
    vector_backend: str = field(
        default_factory=lambda: _env("LAKEHOUSE_VECTOR_BACKEND", "local_hybrid")
    )
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    local_data_dir: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_LOCAL_DATA_DIR", ".local_data")).resolve()
    )
    raw_store_dir: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_RAW_STORE_DIR", ".local_data/raw_assets")).resolve()
    )
    warehouse_dir: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_WAREHOUSE_DIR", ".local_data/warehouse")).resolve()
    )
    wal_dir: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_WAL_DIR", ".local_data/wal")).resolve()
    )
    index_dir: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_INDEX_DIR", ".local_data/indexes")).resolve()
    )
    lineage_dir: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_LINEAGE_DIR", ".local_data/lineage")).resolve()
    )
    sqlite_catalog_path: Path = field(
        default_factory=lambda: Path(_env("LAKEHOUSE_CATALOG_PATH", ".local_data/iceberg_catalog.db")).resolve()
    )
    # Distributed endpoints (Docker / Kubernetes)
    kafka_bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    qdrant_url: str = field(default_factory=lambda: _env("QDRANT_URL", "http://localhost:6333"))
    opensearch_url: str = field(default_factory=lambda: _env("OPENSEARCH_URL", "http://localhost:9200"))
    tika_url: str = field(default_factory=lambda: _env("TIKA_URL", "http://localhost:9998"))
    iceberg_rest_uri: str = field(default_factory=lambda: _env("ICEBERG_REST_URI", "http://localhost:8181/catalog"))
    s3_endpoint_url: str = field(default_factory=lambda: _env("S3_ENDPOINT_URL", "http://localhost:9000"))
    openlineage_url: str = field(default_factory=lambda: _env("OPENLINEAGE_URL", ""))
    openlineage_namespace: str = "ai-native-multimodal-lakehouse"
    # API & Persona RBAC Tokens
    api_host: str = field(default_factory=lambda: _env("LAKEHOUSE_API_HOST", "127.0.0.1"))
    api_port: int = field(default_factory=lambda: _env_int("LAKEHOUSE_API_PORT", 8020))
    admin_token: str = field(default_factory=lambda: _env("RAG_ADMIN_TOKEN", "dev-rag-admin-token"))
    eng_lead_token: str = field(default_factory=lambda: _env("RAG_ENG_LEAD_TOKEN", "dev-rag-eng-token"))
    finance_token: str = field(default_factory=lambda: _env("RAG_FINANCE_TOKEN", "dev-rag-finance-token"))
    public_token: str = field(default_factory=lambda: _env("RAG_PUBLIC_TOKEN", "dev-rag-public-token"))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    def ensure_local_dirs(self) -> None:
        for d in (
            self.local_data_dir,
            self.raw_store_dir,
            self.warehouse_dir,
            self.wal_dir,
            self.index_dir,
            self.lineage_dir,
            self.sqlite_catalog_path.parent,
        ):
            d.mkdir(parents=True, exist_ok=True)


def load_config() -> LakehouseConfig:
    cfg = LakehouseConfig()
    cfg.ensure_local_dirs()
    return cfg
