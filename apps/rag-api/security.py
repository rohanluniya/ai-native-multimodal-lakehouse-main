"""Persona-Aware RBAC, Pre-Retrieval AccessPrincipal Resolution, Rate Limiting & Audit Logging."""

from __future__ import annotations

import hmac
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, HTTPException, Request, status

from multimodal_lakehouse.config import LakehouseConfig, load_config
from multimodal_lakehouse.observability import StructuredLogger
from multimodal_lakehouse.retrieval import AccessPrincipal

ROLE_RANK: dict[str, int] = {
    "public_user": 0,
    "finance_analyst": 1,
    "eng_lead": 2,
    "admin": 3,
}

PERSONA_PROFILES: dict[str, AccessPrincipal] = {
    "public_user": AccessPrincipal("usr-public", "public_user", ("public",), "public"),
    "eng_lead": AccessPrincipal("usr-eng", "eng_lead", ("public", "engineering"), "internal"),
    "finance_analyst": AccessPrincipal("usr-fin", "finance_analyst", ("public", "finance"), "confidential"),
    "admin": AccessPrincipal("usr-admin", "admin", ("public", "engineering", "finance", "admin"), "restricted"),
}


class RateLimiter:
    def __init__(self, max_requests: int = 1200, window_seconds: int = 60) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)

    def check(self, key: str) -> None:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        bucket = [t for t in self._hits[key] if t >= cutoff]
        if len(bucket) >= self.max_requests:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")
        bucket.append(now)
        self._hits[key] = bucket


_limiter = RateLimiter()
_audit = StructuredLogger("rag-api", "security")
AUDIT_TRAIL: list[dict[str, Any]] = []


def record_audit(principal: AccessPrincipal, action: str, path: str, outcome: str, details: dict[str, Any] | None = None) -> None:
    entry = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "principal_id": principal.principal_id,
        "role": principal.role,
        "max_clearance": principal.max_clearance,
        "action": action,
        "path": path,
        "outcome": outcome,
        "details": details or {},
    }
    AUDIT_TRAIL.append(entry)
    if len(AUDIT_TRAIL) > 200:
        del AUDIT_TRAIL[:-200]


def _extract_token(request: Request) -> str | None:
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    key = request.headers.get("x-api-key")
    return key.strip() if key else None


def resolve_principal(request: Request, config: LakehouseConfig | None = None, *, require_auth: bool = False) -> AccessPrincipal:
    cfg = config or load_config()
    ip = request.client.host if request.client else "unknown"
    _limiter.check(ip)

    token = _extract_token(request)
    if not token:
        if require_auth:
            _audit.warning("security.missing_token", "Unauthenticated access rejected", error_code="AUTH_REQUIRED", context={"path": request.url.path})
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication bearer token required")
        return PERSONA_PROFILES["public_user"]

    if hmac.compare_digest(token, cfg.admin_token):
        return PERSONA_PROFILES["admin"]
    if hmac.compare_digest(token, cfg.eng_lead_token):
        return PERSONA_PROFILES["eng_lead"]
    if hmac.compare_digest(token, cfg.finance_token):
        return PERSONA_PROFILES["finance_analyst"]
    if hmac.compare_digest(token, cfg.public_token):
        return PERSONA_PROFILES["public_user"]

    _audit.warning("security.invalid_token", "Invalid bearer credential rejected", error_code="AUTH_INVALID", context={"path": request.url.path})
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bearer token")


def require_authenticated(min_role: str = "public_user"):
    def _dep(request: Request) -> AccessPrincipal:
        p = resolve_principal(request, require_auth=True)
        if ROLE_RANK[p.role] < ROLE_RANK[min_role]:
            record_audit(p, "access_denied", request.url.path, "FORBIDDEN", {"required": min_role})
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Role '{min_role}' required")
        return p

    return Depends(_dep)
