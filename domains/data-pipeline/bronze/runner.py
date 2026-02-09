"""
Runner Bronze: ingestao Oracle -> ClickHouse track_bronze.
Invocado pelo orquestrador (pipelines.yaml).
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[3]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from apps.orchestrator.snapshot_hash_delta_pipeline import run_pipeline


def run():
    run_pipeline(layer="bronze")
