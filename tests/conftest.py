"""Shared pytest fixtures for Project 03 isolated per-test storage."""

from __future__ import annotations

from pathlib import Path

import pytest

from multimodal_lakehouse.config import LakehouseConfig


@pytest.fixture
def isolated_config(tmp_path: Path) -> LakehouseConfig:
    data = tmp_path / "data"
    cfg = LakehouseConfig(
        env="test",
        execution_profile="local_native",
        transport_mode="local_wal",
        vector_backend="local_hybrid",
        local_data_dir=data,
        raw_store_dir=data / "raw_assets",
        warehouse_dir=data / "warehouse",
        wal_dir=data / "wal",
        index_dir=data / "indexes",
        lineage_dir=data / "lineage",
        sqlite_catalog_path=data / "iceberg_catalog.db",
    )
    cfg.ensure_local_dirs()
    return cfg
