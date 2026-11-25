"""Observability: structured JSON logs with secret/PII scrubbing, Prometheus metrics, OTel spans, and OpenLineage v2."""

from __future__ import annotations

import json
import os
import re
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_SENSITIVE_KEY_RE = re.compile(
    r"(token|secret|password|authorization|api_key|credential|cookie|private_key|ssn|credit_card)",
    re.IGNORECASE,
)
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_OTEL_INIT = False


def scrub_sensitive(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: ("[REDACTED]" if _SENSITIVE_KEY_RE.search(str(k)) else scrub_sensitive(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [scrub_sensitive(x) for x in obj]
    if isinstance(obj, str):
        redacted = _SSN_RE.sub("[REDACTED-SSN]", obj)
        return redacted[:512] + "...[TRUNCATED]" if len(redacted) > 512 else redacted
    return obj


def new_trace_context() -> tuple[str, str]:
    return uuid.uuid4().hex, uuid.uuid4().hex[:16]


def get_tracer(name: str = "ai-native-multimodal-lakehouse") -> trace.Tracer:
    global _OTEL_INIT
    if not _OTEL_INIT:
        provider = TracerProvider(resource=Resource.create({"service.name": name}))
        trace.set_tracer_provider(provider)
        _OTEL_INIT = True
    return trace.get_tracer(name)


class StructuredLogger:
    def __init__(self, service: str, component: str, environment: str | None = None) -> None:
        self.service = service
        self.component = component
        self.environment = environment or os.environ.get("LAKEHOUSE_ENV", "dev")

    def emit(
        self,
        severity: str,
        event_type: str,
        message: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        document_id: str | None = None,
        run_id: str | None = None,
        error_code: str = "NONE",
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        tid, cid = new_trace_context()
        rec = {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": self.service,
            "component": self.component,
            "environment": self.environment,
            "severity": severity,
            "event_type": event_type,
            "trace_id": trace_id or tid,
            "correlation_id": correlation_id or cid,
            "document_id": document_id,
            "run_id": run_id or "none",
            "error_code": error_code,
            "message": message,
            "context": scrub_sensitive(context or {}),
        }
        sys.stdout.write(json.dumps(rec, default=str) + "\n")
        sys.stdout.flush()
        return rec

    def info(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        return self.emit("INFO", event_type, message, **kw)

    def warning(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        return self.emit("WARNING", event_type, message, **kw)

    def error(self, event_type: str, message: str, **kw: Any) -> dict[str, Any]:
        return self.emit("ERROR", event_type, message, **kw)


class PlatformMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.assets_ingested_total = Counter(
            "multimodal_assets_ingested_total",
            "Total multimodal assets ingested by modality and connector",
            ["modality", "connector"],
            registry=self.registry,
        )
        self.quarantined_assets_total = Counter(
            "multimodal_quarantined_assets_total",
            "Corrupt, schema-invalid, or poisoned assets routed to quarantine",
            ["reason"],
            registry=self.registry,
        )
        self.pii_redactions_total = Counter(
            "multimodal_pii_redactions_total",
            "PII entities detected and redacted before embedding",
            ["pii_type"],
            registry=self.registry,
        )
        self.chunks_indexed_total = Counter(
            "multimodal_chunks_indexed_total",
            "Versioned chunks indexed into lexical and vector stores",
            ["model_version", "strategy"],
            registry=self.registry,
        )
        self.retrieval_latency_seconds = Histogram(
            "multimodal_retrieval_latency_seconds",
            "ACL-filtered hybrid retrieval latency in seconds",
            ["mode"],
            buckets=(0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
            registry=self.registry,
        )
        self.acl_denied_chunks_total = Counter(
            "multimodal_acl_denied_chunks_total",
            "Chunks filtered out prior to retrieval scoring due to ACL policy",
            ["security_tier"],
            registry=self.registry,
        )
        self.deletions_propagated_total = Counter(
            "multimodal_deletions_propagated_total",
            "Right-to-be-forgotten deletion tombstones propagated across all stores",
            ["status"],
            registry=self.registry,
        )
        self.active_corpus_chunks = Gauge(
            "multimodal_active_corpus_chunks",
            "Currently active non-deleted chunks in Gold retrieval corpus",
            registry=self.registry,
        )
        self.dq_checks_total = Counter(
            "multimodal_dq_checks_total",
            "Data quality check executions",
            ["check_name", "status"],
            registry=self.registry,
        )
        self.api_requests_total = Counter(
            "multimodal_api_requests_total",
            "FastAPI requests served",
            ["endpoint", "method", "status_code"],
            registry=self.registry,
        )
        self.api_latency_seconds = Histogram(
            "multimodal_api_latency_seconds",
            "API request duration in seconds",
            ["endpoint"],
            registry=self.registry,
        )

    def render_prometheus(self) -> bytes:
        return generate_latest(self.registry)


_METRICS_SINGLETON: PlatformMetrics | None = None


def get_metrics() -> PlatformMetrics:
    global _METRICS_SINGLETON
    if _METRICS_SINGLETON is None:
        _METRICS_SINGLETON = PlatformMetrics()
    return _METRICS_SINGLETON


class OpenLineageEmitter:
    def __init__(self, namespace: str, marquez_url: str = "", local_dir: Path | None = None) -> None:
        self.namespace = namespace
        self.marquez_url = marquez_url.rstrip("/") if marquez_url else ""
        self.local_dir = local_dir
        if local_dir is not None:
            local_dir.mkdir(parents=True, exist_ok=True)

    def emit(
        self,
        *,
        event_type: str,
        job_name: str,
        run_id: str | None = None,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "eventType": event_type.upper(),
            "eventTime": datetime.now(UTC).isoformat(),
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "producer": "https://github.com/ai-native-multimodal-lakehouse/openlineage/1.0.0",
            "run": {"runId": run_id or str(uuid.uuid4()), "facets": {}},
            "job": {"namespace": self.namespace, "name": job_name, "facets": {}},
            "inputs": [{"namespace": self.namespace, "name": i["name"], "facets": {}} for i in (inputs or [])],
            "outputs": [{"namespace": self.namespace, "name": o["name"], "facets": {}} for o in (outputs or [])],
        }
        if self.local_dir is not None:
            with (self.local_dir / "openlineage_events.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload) + "\n")
        if self.marquez_url:
            try:
                requests.post(f"{self.marquez_url}/api/v1/lineage", json=payload, timeout=2.0)
            except Exception:
                pass
        return payload
