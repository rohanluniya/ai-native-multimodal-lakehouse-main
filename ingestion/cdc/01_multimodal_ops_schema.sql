-- Operational Document Registry & ACL Permission Database (PostgreSQL 17)
-- Source for Debezium 3.7.1 Logical Replication CDC into Kafka

CREATE SCHEMA IF NOT EXISTS multimodal_ops;

CREATE TABLE IF NOT EXISTS multimodal_ops.documents (
    document_id VARCHAR(64) PRIMARY KEY,
    current_version INTEGER NOT NULL DEFAULT 1,
    title VARCHAR(256) NOT NULL,
    modality VARCHAR(32) NOT NULL,
    source_uri TEXT NOT NULL,
    content_sha256 VARCHAR(64) NOT NULL,
    security_tier VARCHAR(24) NOT NULL CHECK (security_tier IN ('public', 'internal', 'confidential', 'restricted')),
    acl_allow_groups TEXT[] NOT NULL DEFAULT ARRAY['public'],
    acl_deny_groups TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS multimodal_ops.deletion_requests (
    tombstone_id UUID PRIMARY KEY,
    document_id VARCHAR(64) NOT NULL,
    reason VARCHAR(128) NOT NULL,
    requested_by VARCHAR(64) NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'multimodal_cdc_pub') THEN
        CREATE PUBLICATION multimodal_cdc_pub FOR TABLE
            multimodal_ops.documents,
            multimodal_ops.deletion_requests;
    END IF;
END $$;
