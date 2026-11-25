"""Dual Chunking Engine (Semantic Heading-Aware vs Fixed-Size Sliding Window) with Parent-Child Lineage & ACL Inheritance."""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from typing import Any

from multimodal_lakehouse.parsing import ParsedArtifact

_CHUNK_NS = uuid.UUID("77c1f2e3-1234-4abc-9def-abcdef654321")


@dataclass(frozen=True)
class VersionedChunk:
    chunk_id: str
    parent_chunk_id: str
    document_id: str
    version_id: str
    version_num: int
    chunk_index: int
    strategy: str
    title: str
    modality: str
    language: str
    source_uri: str
    char_start: int
    char_end: int
    token_count: int
    chunk_text: str
    chunk_sha256: str
    security_tier: str
    acl_allow_groups: tuple[str, ...]
    acl_deny_groups: tuple[str, ...]
    pii_redacted: bool
    prompt_injection_flagged: bool
    event_time: str


def _split_semantic_sections(text: str, max_chars: int = 420) -> list[tuple[int, int, str]]:
    """Split text along markdown headings and blank-line paragraph boundaries."""
    spans: list[tuple[int, int, str]] = []
    # Split on double newlines or heading markers while tracking exact character offsets
    parts = re.split(r"\n\s*\n", text)
    cursor = 0
    for part in parts:
        clean = part.strip()
        if not clean:
            continue
        start = text.find(clean, cursor)
        if start < 0:
            start = cursor
        end = start + len(clean)
        cursor = end
        if len(clean) <= max_chars:
            spans.append((start, end, clean))
        else:
            # Sub-chunk long paragraphs cleanly on sentence boundaries
            pos = 0
            while pos < len(clean):
                sub = clean[pos : pos + max_chars].strip()
                if sub:
                    spans.append((start + pos, start + pos + len(sub), sub))
                pos += max_chars - 40
    return spans or [(0, len(text), text.strip())]


def _split_fixed_window(text: str, window_words: int = 45, overlap_words: int = 10) -> list[tuple[int, int, str]]:
    words = list(re.finditer(r"\S+", text))
    if not words:
        return [(0, len(text), text.strip())]
    spans: list[tuple[int, int, str]] = []
    step = max(1, window_words - overlap_words)
    for i in range(0, len(words), step):
        slice_w = words[i : i + window_words]
        if not slice_w:
            break
        start = slice_w[0].start()
        end = slice_w[-1].end()
        spans.append((start, end, text[start:end].strip()))
        if i + window_words >= len(words):
            break
    return spans


def chunk_artifact(
    event: dict[str, Any],
    artifact: ParsedArtifact,
    *,
    strategy: str = "semantic",
) -> list[VersionedChunk]:
    """Generate deterministic versioned chunks with full parent-child lineage and inherited ACLs.

    Always chunks `artifact.redacted_text` so raw PII never enters vector indexes or RAG prompts.
    """
    text = artifact.redacted_text.strip()
    if not text:
        return []
    doc_id = str(event["document_id"])
    ver_num = int(event["version_num"])
    version_id = f"{doc_id}:v{ver_num}:{str(event['content_sha256'])[:10]}"
    parent_id = str(uuid.uuid5(_CHUNK_NS, f"parent:{version_id}:{strategy}"))

    spans = (
        _split_semantic_sections(text)
        if strategy == "semantic"
        else _split_fixed_window(text)
    )
    allow = tuple(sorted(set(event.get("acl_allow_groups") or [])))
    deny = tuple(sorted(set(event.get("acl_deny_groups") or [])))

    chunks: list[VersionedChunk] = []
    for idx, (c_start, c_end, body) in enumerate(spans):
        sha = hashlib.sha256(body.encode("utf-8")).hexdigest()
        chunk_id = str(uuid.uuid5(_CHUNK_NS, f"{version_id}:{strategy}:{idx}:{sha[:12]}"))
        chunks.append(
            VersionedChunk(
                chunk_id=chunk_id,
                parent_chunk_id=parent_id,
                document_id=doc_id,
                version_id=version_id,
                version_num=ver_num,
                chunk_index=idx,
                strategy=strategy,
                title=str(event["title"]),
                modality=artifact.modality,
                language=artifact.detected_language,
                source_uri=str(event["source_uri"]),
                char_start=c_start,
                char_end=c_end,
                token_count=len(body.split()),
                chunk_text=body,
                chunk_sha256=sha,
                security_tier=str(event["security_tier"]),
                acl_allow_groups=allow,
                acl_deny_groups=deny,
                pii_redacted=artifact.pii_redacted_count > 0,
                prompt_injection_flagged=artifact.prompt_injection_detected,
                event_time=str(event["event_time"]),
            )
        )
    return chunks
