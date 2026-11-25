"""Apache Spark 4.1.4 + Apache Iceberg 1.12.0 + OpenLineage 1.55.0 Multimodal Lakehouse Curation Job."""

from __future__ import annotations

import json
import os
import time
from typing import Any

try:
    from pyspark.sql import SparkSession
except ImportError as exc:  # pragma: no cover - container-only Spark runtime
    raise SystemExit("Run inside Spark 4.1.4 container (`docker compose --profile analytics up`).") from exc


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("multimodal-lakehouse-iceberg-batch")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", os.getenv("ICEBERG_REST_URI", "http://lakekeeper:8181/catalog"))
        .config("spark.sql.catalog.lakehouse.warehouse", "multimodal")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", os.getenv("S3_ENDPOINT_URL", "http://rustfs:9000"))
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", os.getenv("OPENLINEAGE_URL", "http://marquez:5000"))
        .config("spark.openlineage.namespace", "ai-native-multimodal-lakehouse")
        .getOrCreate()
    )


def curate_active_corpus(spark: SparkSession) -> dict[str, Any]:
    t0 = time.perf_counter()
    spark.sql(
        """
        MERGE INTO lakehouse.gold.retrieval_corpus_active AS tgt
        USING (
            SELECT c.*
            FROM lakehouse.silver.chunks_lineage AS c
            JOIN lakehouse.silver.document_versions_scd2 AS d
              ON c.version_id = d.version_id
            WHERE d.is_current = true AND d.is_deleted = false
        ) AS src
        ON tgt.chunk_id = src.chunk_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.sql(
        """
        DELETE FROM lakehouse.gold.retrieval_corpus_active
        WHERE document_id IN (SELECT DISTINCT document_id FROM lakehouse.silver.deletion_tombstones)
        """
    )
    spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'gold.retrieval_corpus_active')")
    cnt = spark.table("lakehouse.gold.retrieval_corpus_active").count()
    return {"active_chunks": cnt, "duration_seconds": round(time.perf_counter() - t0, 3)}


if __name__ == "__main__":
    s = build_spark()
    try:
        print(json.dumps(curate_active_corpus(s), indent=2))
    finally:
        s.stop()
