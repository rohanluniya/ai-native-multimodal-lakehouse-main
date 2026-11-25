"""Self-Hosted Zero-Download Embedding Registry, Pre-Retrieval ACL Filter, Hybrid BM25 + Vector Index, Reranker, Re-Embedding Migration & Tombstone Purge.

Compliance note:
- Uses 100% self-contained deterministic scikit-learn / NumPy TF-IDF + TruncatedSVD embeddings
  (zero external pretrained weight downloads, zero third-party model license risks).
- Exposes dual model versions (`embed-v1-tfidf-svd-128d` and `embed-v2-charword-svd-256d`) to
  prove real blue/green re-embedding migration and retrieval quality lift.
- Enforces strict PRE-RETRIEVAL ACL filtering (deny-overrides-allow + security tier clearance)
  before scoring or disclosing any passage.
"""

from __future__ import annotations

import hashlib
import json
import math
import pickle
import re
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from multimodal_lakehouse.chunking import VersionedChunk
from multimodal_lakehouse.observability import get_metrics, get_tracer

TIER_CLEARANCE: dict[str, int] = {
    "public": 0,
    "internal": 1,
    "confidential": 2,
    "restricted": 3,
}


@dataclass(frozen=True)
class AccessPrincipal:
    principal_id: str
    role: str
    groups: tuple[str, ...]
    max_clearance: str  # public | internal | confidential | restricted


@dataclass(frozen=True)
class EmbeddingModelSpec:
    model_version: str
    dimensions: int
    analyzer: str
    ngram_range: tuple[int, int]
    description: str


MODEL_REGISTRY: dict[str, EmbeddingModelSpec] = {
    "embed-v1-tfidf-svd-128d": EmbeddingModelSpec(
        model_version="embed-v1-tfidf-svd-128d",
        dimensions=128,
        analyzer="word",
        ngram_range=(1, 1),
        description="Baseline unigram word TF-IDF + L2-normalized dense SVD projection (128d)",
    ),
    "embed-v2-charword-svd-256d": EmbeddingModelSpec(
        model_version="embed-v2-charword-svd-256d",
        dimensions=256,
        analyzer="char_wb",
        ngram_range=(3, 5),
        description="Upgraded subword char_wb (3-5) TF-IDF + dense SVD projection (256d) improving multilingual & acronym recall",
    ),
}


def is_chunk_authorized(chunk: dict[str, Any], principal: AccessPrincipal) -> bool:
    """Strict pre-retrieval ACL evaluation: deny-overrides-allow + tier clearance."""
    if principal.role == "admin":
        return True
    p_groups = set(principal.groups)
    deny_groups = set(chunk.get("acl_deny_groups") or [])
    if p_groups & deny_groups:
        return False
    req_tier = TIER_CLEARANCE.get(str(chunk.get("security_tier", "restricted")), 3)
    user_tier = TIER_CLEARANCE.get(principal.max_clearance, 0)
    if user_tier < req_tier:
        return False
    allow_groups = set(chunk.get("acl_allow_groups") or [])
    return bool(p_groups & allow_groups)


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9À-ÿ_-]{2,}", text.lower())


class HybridRetrievalIndex:
    """Persistent local BM25 + Dense Vector index with blue/green model versions, query cache & tombstone purge."""

    def __init__(self, index_dir: Path) -> None:
        self.index_dir = index_dir
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.metrics = get_metrics()
        self.tracer = get_tracer()
        self.active_model_version: str = "embed-v1-tfidf-svd-128d"
        self.chunks: list[dict[str, Any]] = []
        self._chunk_by_id: dict[str, dict[str, Any]] = {}
        # BM25 structures
        self._doc_tokens: list[list[str]] = []
        self._doc_tf: list[Counter[str]] = []
        self._df: Counter[str] = Counter()
        self._avgdl: float = 1.0
        # Versioned vector spaces (supports simultaneous blue/green model_versions!)
        self._vectorizers: dict[str, TfidfVectorizer] = {}
        self._svds: dict[str, TruncatedSVD | None] = {}
        self._embeddings: dict[str, np.ndarray] = {}
        self._index_metadata: dict[str, dict[str, Any]] = {}
        # Query response cache (invalidated immediately on deletion tombstones or re-embedding)
        self._query_cache: dict[str, dict[str, Any]] = {}

    @property
    def state_path(self) -> Path:
        return self.index_dir / "hybrid_index_state.pkl"

    def build_index(
        self,
        chunks: list[VersionedChunk] | list[dict[str, Any]],
        *,
        model_version: str = "embed-v1-tfidf-svd-128d",
        activate: bool = True,
    ) -> dict[str, Any]:
        """Build BM25 lexical index + versioned dense vector embeddings in batched numpy arrays."""
        t0 = time.perf_counter()
        if model_version not in MODEL_REGISTRY:
            raise ValueError(f"Unknown model_version={model_version}")
        spec = MODEL_REGISTRY[model_version]

        norm_chunks: list[dict[str, Any]] = []
        for c in chunks:
            if isinstance(c, VersionedChunk):
                d = {
                    "chunk_id": c.chunk_id,
                    "parent_chunk_id": c.parent_chunk_id,
                    "document_id": c.document_id,
                    "version_id": c.version_id,
                    "version_num": c.version_num,
                    "chunk_index": c.chunk_index,
                    "strategy": c.strategy,
                    "title": c.title,
                    "modality": c.modality,
                    "language": c.language,
                    "source_uri": c.source_uri,
                    "char_start": c.char_start,
                    "char_end": c.char_end,
                    "token_count": c.token_count,
                    "chunk_text": c.chunk_text,
                    "chunk_sha256": c.chunk_sha256,
                    "security_tier": c.security_tier,
                    "acl_allow_groups": list(c.acl_allow_groups),
                    "acl_deny_groups": list(c.acl_deny_groups),
                    "pii_redacted": c.pii_redacted,
                    "prompt_injection_flagged": c.prompt_injection_flagged,
                    "event_time": c.event_time,
                }
            else:
                d = dict(c)
            norm_chunks.append(d)

        self.chunks = norm_chunks
        self._chunk_by_id = {c["chunk_id"]: c for c in self.chunks}
        self._rebuild_bm25()

        corpus_texts = [f"{c['title']} {c['chunk_text']}" for c in self.chunks]
        if corpus_texts:
            vec = TfidfVectorizer(
                analyzer=spec.analyzer,
                ngram_range=spec.ngram_range,
                max_features=8192,
                sublinear_tf=True,
            )
            X = vec.fit_transform(corpus_texts)
            max_comp = min(spec.dimensions, X.shape[0] - 1, X.shape[1] - 1)
            svd: Any = None
            if max_comp >= 4:
                svd_model = TruncatedSVD(n_components=max_comp, random_state=42)
                dense = svd_model.fit_transform(X).astype(np.float32)
                svd = svd_model
            else:
                dense = X.toarray().astype(np.float32)
            dense = normalize(dense, norm="l2", axis=1)
            self._vectorizers[model_version] = vec
            self._svds[model_version] = svd
            self._embeddings[model_version] = dense
            eff_dims = int(dense.shape[1])
        else:
            eff_dims = spec.dimensions
            self._embeddings[model_version] = np.zeros((0, eff_dims), dtype=np.float32)

        dur_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        self._index_metadata[model_version] = {
            "model_version": model_version,
            "configured_dimensions": spec.dimensions,
            "effective_dimensions": eff_dims,
            "indexed_chunks": len(self.chunks),
            "build_duration_ms": dur_ms,
            "description": spec.description,
        }
        if activate:
            self.active_model_version = model_version
        self._query_cache.clear()
        self.metrics.chunks_indexed_total.labels(
            model_version=model_version,
            strategy=self.chunks[0]["strategy"] if self.chunks else "semantic",
        ).inc(len(self.chunks))
        self.metrics.active_corpus_chunks.set(len(self.chunks))
        self.save()
        return self._index_metadata[model_version]

    def _rebuild_bm25(self) -> None:
        self._doc_tokens = [_tokenize(f"{c['title']} {c['chunk_text']}") for c in self.chunks]
        self._doc_tf = [Counter(toks) for toks in self._doc_tokens]
        self._df = Counter()
        for tf in self._doc_tf:
            for term in tf:
                self._df[term] += 1
        total_len = sum(len(t) for t in self._doc_tokens)
        self._avgdl = total_len / max(1, len(self._doc_tokens))

    def migrate_reembed(self, target_model_version: str = "embed-v2-charword-svd-256d") -> dict[str, Any]:
        """Zero-downtime blue/green re-embedding migration into a new versioned vector space."""
        previous_version = self.active_model_version
        meta = self.build_index(self.chunks, model_version=target_model_version, activate=True)
        return {
            "previous_model_version": previous_version,
            "active_model_version": self.active_model_version,
            "chunks_reembedded": len(self.chunks),
            "migration_duration_ms": meta["build_duration_ms"],
            "effective_dimensions": meta["effective_dimensions"],
            "status": "MIGRATED",
        }

    def purge_document(self, document_id: str) -> dict[str, Any]:
        """Immediately purge a deleted document from BM25, ALL vector model versions, and query cache."""
        t0 = time.perf_counter()
        keep_indices = [i for i, c in enumerate(self.chunks) if c["document_id"] != document_id]
        removed = len(self.chunks) - len(keep_indices)
        if removed > 0:
            self.chunks = [self.chunks[i] for i in keep_indices]
            self._chunk_by_id = {c["chunk_id"]: c for c in self.chunks}
            self._rebuild_bm25()
            for mver, emb in list(self._embeddings.items()):
                if emb.shape[0] >= max(keep_indices, default=0):
                    self._embeddings[mver] = emb[keep_indices] if keep_indices else np.zeros((0, emb.shape[1]), dtype=np.float32)
                    if mver in self._index_metadata:
                        self._index_metadata[mver]["indexed_chunks"] = len(self.chunks)
            self._query_cache.clear()
            self.metrics.active_corpus_chunks.set(len(self.chunks))
            self.save()
        return {
            "document_id": document_id,
            "chunks_purged": removed,
            "cache_invalidated": True,
            "purge_duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
        }

    def _bm25_scores(self, query: str, allowed_indices: list[int]) -> dict[int, float]:
        q_terms = _tokenize(query)
        n_docs = max(1, len(self.chunks))
        k1, b = 1.5, 0.75
        scores: dict[int, float] = {}
        for idx in allowed_indices:
            tf_map = self._doc_tf[idx]
            dl = len(self._doc_tokens[idx])
            s = 0.0
            for term in q_terms:
                f = tf_map.get(term, 0)
                if f == 0:
                    continue
                df = self._df.get(term, 0)
                idf = math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))
                s += idf * ((f * (k1 + 1.0)) / (f + k1 * (1.0 - b + b * (dl / max(1.0, self._avgdl)))))
            if s > 0.0:
                scores[idx] = s
        return scores

    def _vector_scores(self, query: str, allowed_indices: list[int], model_version: str) -> dict[int, float]:
        vec = self._vectorizers.get(model_version)
        emb = self._embeddings.get(model_version)
        if vec is None or emb is None or emb.shape[0] == 0 or not allowed_indices:
            return {}
        qx = vec.transform([query])
        svd = self._svds.get(model_version)
        q_dense = svd.transform(qx).astype(np.float32) if svd is not None else qx.toarray().astype(np.float32)
        q_dense = normalize(q_dense, norm="l2", axis=1)[0]
        sub = emb[allowed_indices]
        sims = sub @ q_dense
        return {allowed_indices[i]: float(sims[i]) for i in range(len(allowed_indices)) if float(sims[i]) > 0.0}

    def query(
        self,
        query_text: str,
        principal: AccessPrincipal,
        *,
        top_k: int = 5,
        mode: str = "hybrid_rerank",  # bm25 | vector | hybrid | hybrid_rerank
        model_version: str | None = None,
        modality_filter: str | None = None,
        language_filter: str | None = None,
        exclude_prompt_injections: bool = True,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        """ACL-first hybrid retrieval with provenance citations and prompt-injection protection."""
        t0 = time.perf_counter()
        mver = model_version or self.active_model_version
        cache_key = hashlib.sha256(
            json.dumps([query_text, principal.principal_id, principal.role, principal.groups, principal.max_clearance, top_k, mode, mver, modality_filter, language_filter, exclude_prompt_injections]).encode()
        ).hexdigest()
        if use_cache and cache_key in self._query_cache:
            cached = dict(self._query_cache[cache_key])
            cached["cache_hit"] = True
            return cached

        with self.tracer.start_as_current_span("hybrid_retrieval.query") as span:
            span.set_attribute("retrieval.mode", mode)
            span.set_attribute("retrieval.model_version", mver)
            span.set_attribute("principal.role", principal.role)

            # STEP 1: STRICT PRE-RETRIEVAL ACL + METADATA FILTERING (BEFORE SCORING!)
            allowed_indices: list[int] = []
            acl_denied_count = 0
            injection_blocked_count = 0
            for i, c in enumerate(self.chunks):
                if not is_chunk_authorized(c, principal):
                    acl_denied_count += 1
                    self.metrics.acl_denied_chunks_total.labels(security_tier=c["security_tier"]).inc()
                    continue
                if modality_filter and c["modality"] != modality_filter:
                    continue
                if language_filter and c["language"] != language_filter:
                    continue
                if exclude_prompt_injections and c.get("prompt_injection_flagged"):
                    injection_blocked_count += 1
                    continue
                allowed_indices.append(i)

            bm25 = self._bm25_scores(query_text, allowed_indices) if mode in ("bm25", "hybrid", "hybrid_rerank") else {}
            vec = self._vector_scores(query_text, allowed_indices, mver) if mode in ("vector", "hybrid", "hybrid_rerank") else {}

            ranked_bm25 = [idx for idx, _ in sorted(bm25.items(), key=lambda kv: kv[1], reverse=True)[:50]]
            ranked_vec = [idx for idx, _ in sorted(vec.items(), key=lambda kv: kv[1], reverse=True)[:50]]

            fused: dict[int, float] = {}
            if mode == "bm25":
                fused = dict(bm25)
            elif mode == "vector":
                fused = dict(vec)
            else:
                # Reciprocal Rank Fusion (RRF, k=60)
                rrf_k = 60.0
                for rank, idx in enumerate(ranked_bm25, 1):
                    fused[idx] = fused.get(idx, 0.0) + 1.0 / (rrf_k + rank)
                for rank, idx in enumerate(ranked_vec, 1):
                    fused[idx] = fused.get(idx, 0.0) + 1.0 / (rrf_k + rank)

                if mode == "hybrid_rerank":
                    q_toks = set(_tokenize(query_text))
                    for idx in list(fused.keys()):
                        c = self.chunks[idx]
                        title_overlap = len(q_toks & set(_tokenize(c["title"]))) / max(1, len(q_toks))
                        # Boost exact title matches + latest revisions; demote untrusted chunks if ever unexcluded
                        fused[idx] += 0.012 * title_overlap + 0.0015 * min(3, int(c["version_num"]))
                        if c.get("prompt_injection_flagged"):
                            fused[idx] *= 0.15

            top_items = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)[:top_k]
            hits: list[dict[str, Any]] = []
            for rank, (idx, score) in enumerate(top_items, 1):
                c = self.chunks[idx]
                hits.append(
                    {
                        "rank": rank,
                        "score": round(score, 5),
                        "bm25_score": round(bm25.get(idx, 0.0), 4),
                        "vector_score": round(vec.get(idx, 0.0), 4),
                        "chunk_id": c["chunk_id"],
                        "parent_chunk_id": c["parent_chunk_id"],
                        "document_id": c["document_id"],
                        "version_id": c["version_id"],
                        "version_num": c["version_num"],
                        "title": c["title"],
                        "modality": c["modality"],
                        "language": c["language"],
                        "security_tier": c["security_tier"],
                        "source_uri": c["source_uri"],
                        "char_span": [c["char_start"], c["char_end"]],
                        "pii_redacted": c["pii_redacted"],
                        "prompt_injection_flagged": c["prompt_injection_flagged"],
                        "passage": c["chunk_text"],
                        "citation": f"[{c['document_id']} v{c['version_num']} · {c['modality'].upper()} · chars {c['char_start']}-{c['char_end']}]",
                    }
                )

            # Deterministic grounded extractive answer strictly synthesized from authorized cited passages
            if hits:
                top_passages = [f"{h['passage'][:220].strip()} {h['citation']}" for h in hits[:2]]
                answer = " ".join(top_passages)
            else:
                answer = "No authorized passages matched your query under your current ACL permissions."

            dur = time.perf_counter() - t0
            self.metrics.retrieval_latency_seconds.labels(mode=mode).observe(dur)
            result = {
                "query": query_text,
                "principal": {
                    "principal_id": principal.principal_id,
                    "role": principal.role,
                    "max_clearance": principal.max_clearance,
                },
                "retrieval_mode": mode,
                "model_version": mver,
                "total_corpus_chunks": len(self.chunks),
                "authorized_candidates": len(allowed_indices),
                "acl_filtered_out": acl_denied_count,
                "prompt_injections_blocked": injection_blocked_count,
                "latency_ms": round(dur * 1000.0, 3),
                "cache_hit": False,
                "grounded_answer": answer,
                "citations": [h["citation"] for h in hits],
                "results": hits,
            }
            if use_cache:
                self._query_cache[cache_key] = result
            return result

    def model_catalog(self) -> list[dict[str, Any]]:
        out = []
        for mver, spec in MODEL_REGISTRY.items():
            meta = self._index_metadata.get(mver, {})
            out.append(
                {
                    "model_version": mver,
                    "active": mver == self.active_model_version,
                    "indexed": mver in self._embeddings,
                    "dimensions": meta.get("effective_dimensions", spec.dimensions),
                    "indexed_chunks": meta.get("indexed_chunks", 0),
                    "build_duration_ms": meta.get("build_duration_ms", 0.0),
                    "description": spec.description,
                }
            )
        return out

    def save(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        with tmp.open("wb") as fh:
            pickle.dump(
                {
                    "active_model_version": self.active_model_version,
                    "chunks": self.chunks,
                    "vectorizers": self._vectorizers,
                    "svds": self._svds,
                    "embeddings": self._embeddings,
                    "index_metadata": self._index_metadata,
                },
                fh,
            )
        tmp.replace(self.state_path)

    def load(self) -> bool:
        if not self.state_path.exists():
            return False
        with self.state_path.open("rb") as fh:
            d = pickle.load(fh)
        self.active_model_version = d["active_model_version"]
        self.chunks = d["chunks"]
        self._chunk_by_id = {c["chunk_id"]: c for c in self.chunks}
        self._vectorizers = d["vectorizers"]
        self._svds = d["svds"]
        self._embeddings = d["embeddings"]
        self._index_metadata = d["index_metadata"]
        self._rebuild_bm25()
        self.metrics.active_corpus_chunks.set(len(self.chunks))
        return True
