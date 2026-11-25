"""End-to-end CLI pipeline runner for Project 03 (Generator -> Bus -> Parsing -> Chunking -> Iceberg V2 -> Hybrid Index -> Re-Embedding -> Eval -> DQ)."""

from __future__ import annotations

import argparse
import json

from multimodal_lakehouse.config import load_config
from multimodal_lakehouse.contracts import AvroContractRegistry
from multimodal_lakehouse.generator import PROFILES, MultimodalCorpusGenerator
from multimodal_lakehouse.lakehouse import MultimodalLakehouse
from multimodal_lakehouse.retrieval import HybridRetrievalIndex
from multimodal_lakehouse.transport import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(description="Run AI-Native Multimodal Lakehouse end-to-end pipeline")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="local")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--strategy", choices=["semantic", "fixed_size"], default="semantic")
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    bus = create_event_bus(cfg)
    reg = AvroContractRegistry(cfg.project_root / "schemas" / "avro")
    lh = MultimodalLakehouse(cfg)
    idx = HybridRetrievalIndex(cfg.index_dir)

    gen_res = MultimodalCorpusGenerator(cfg, bus, reg, profile=args.profile, seed=args.seed).run()
    proc_res = lh.process_from_bus(bus, reg, idx, chunk_strategy=args.strategy, run_migration_v2=True)
    dq_res = lh.run_data_quality(idx)
    comp_res = [lh.compact_table("bronze.raw_assets"), lh.compact_table("silver.chunks_lineage")] if args.compact else []

    print(
        json.dumps(
            {
                "generator": gen_res,
                "lakehouse_and_index": proc_res,
                "data_quality": {"overall_status": dq_res["overall_status"], "summary": dq_res["summary"]},
                "compaction": comp_res,
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
