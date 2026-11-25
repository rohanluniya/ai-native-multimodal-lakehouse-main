# Limitations, Non-Goals & Honest Tradeoffs

1. **Host Execution Environment (`local_native` vs. Distributed Cluster):**
   - All verified benchmarks, tests, and screenshots were executed natively on Apple Silicon (`Darwin 24.6.0`, `arm64`, Python `3.12.8`) using PyIceberg V2 (SQLite catalog + local Parquet warehouse), DuckDB 2.0, local file-backed Kafka spooling, and an in-process hybrid vector/BM25 engine.
   - Because Docker and a local JVM were unavailable on the build workstation, the multi-service `docker-compose.yml` (Kafka 4.3.2, Qdrant v1.20.2, OpenSearch 3.9.0, Apache Tika 3.4.0, RustFS 1.0.6, Lakekeeper v0.15.1, PostGIS 17, Debezium 3.7.1, Spark 4.1.4, Trino 484, Airflow 3.4.0) and `infra/kubernetes/multimodal-platform.yaml` were validated statically (YAML syntax, pinned multi-arch tags, zero `:latest`, script AST checks), not live-booted end-to-end inside containers.

2. **Zero-Download Deterministic Embeddings vs. Pretrained Neural Encoders:**
   - In strict compliance with model-license governance, this repository uses zero-download, self-contained scikit-learn TF-IDF + TruncatedSVD embeddings (`embed-v1-tfidf-svd-128d` and `embed-v2-charword-svd-256d`) rather than downloading third-party neural checkpoints (e.g., BGE, E5, OpenAI, Whisper).
   - While deterministic embeddings make CI fast, reproducible, and offline-safe, cross-lingual semantic recall (e.g., querying in English for a German-only document without lexical overlap) is inherently weaker than a multilingual neural encoder. The embedding interface in `multimodal_lakehouse/retrieval.py` is modular so a vetted GPU encoder can be swapped in behind the same blue/green migration protocol.

3. **OCR & Audio Transcription Scope:**
   - The multimodal parser extracts real embedded PDF text via `pypdf`, real image dimensions/color modes/PNG text metadata via `Pillow`, and real PCM sample rate/channels/duration via `wave`. Running neural OCR (Tesseract/PaddleOCR) or speech-to-text (Whisper) on unannotated raw pixels/waveforms requires dedicated external binaries/model weights and is modeled via the Tika/plugin boundary in `docker-compose.yml`.

4. **Corpus Size & Synthetic Evaluation Ceiling:**
   - The evaluation harness contains 12 deterministic IR and security fixtures over 88–372 synthetic multimodal assets. Reaching `Recall@5=1.0` and `nDCG@5=1.0` proves pipeline, ACL, and ranking plumbing integrity, **not** open-domain web-scale retrieval accuracy on noisy enterprise corpora.

5. **PII Redaction & Prompt-Injection Detection Boundaries:**
   - Regex-based SSN/email redaction and pattern-based prompt-injection screening catch well-defined synthetic patterns (`0` leaks measured), but production deployments should layer NER models (e.g., Presidio) and LLM output guardrails for unstructured PII and obfuscated jailbreaks.
