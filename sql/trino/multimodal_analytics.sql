-- Trino 484 Analytical SQL — AI-Native Multimodal Lakehouse (Iceberg Format V2)

-- 1. Active Corpus Summary by Modality, Language, and Security Tier
SELECT
    modality,
    language,
    security_tier,
    COUNT(DISTINCT document_id) AS documents,
    COUNT(*) AS active_chunks,
    SUM(token_count) AS total_tokens,
    SUM(CASE WHEN pii_redacted THEN 1 ELSE 0 END) AS pii_redacted_chunks,
    SUM(CASE WHEN prompt_injection_flagged THEN 1 ELSE 0 END) AS injection_flagged_chunks
FROM iceberg.gold.retrieval_corpus_active
GROUP BY modality, language, security_tier
ORDER BY active_chunks DESC;

-- 2. Document Revision & ACL SCD Type 2 Audit Trail
SELECT
    document_id,
    version_num,
    security_tier,
    acl_allow_csv,
    acl_deny_csv,
    pii_redacted_count,
    is_current,
    is_deleted,
    effective_from,
    effective_to
FROM iceberg.silver.document_versions_scd2
ORDER BY document_id, version_num;

-- 3. Right-to-be-Forgotten Deletion Tombstone SLA Audit
SELECT
    document_id,
    reason,
    requested_by,
    chunks_purged,
    ROUND(propagation_ms, 2) AS propagation_ms,
    sla_met
FROM iceberg.silver.deletion_tombstones
ORDER BY requested_at DESC;

-- 4. RAG Evaluation Mode & Model Comparison (Recall@1, Recall@5, MRR, nDCG, Security Leakage)
SELECT
    mode,
    model_version,
    ROUND(recall_at_1, 3) AS recall_at_1,
    ROUND(recall_at_5, 3) AS recall_at_5,
    ROUND(mrr_at_5, 3) AS mrr_at_5,
    ROUND(ndcg_at_5, 3) AS ndcg_at_5,
    ROUND(groundedness, 3) AS groundedness,
    acl_leakage_count,
    prompt_injection_leaks,
    deleted_doc_leaks,
    ROUND(p95_latency_ms, 2) AS p95_ms
FROM iceberg.gold.rag_eval_metrics
ORDER BY executed_at DESC, mrr_at_5 DESC;

-- 5. Iceberg Snapshot Time Travel Inspection
SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
FROM iceberg.silver."document_versions_scd2$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
