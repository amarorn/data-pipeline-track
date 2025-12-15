"""
Runner da camada Bronze para o domínio data-pipeline.
Executa snapshots D-1 do Oracle para Bronze.
"""
from __future__ import annotations

import yaml
from pathlib import Path

from track_platform.logging import get_pipeline_logger
from .oracle_ingestion import OracleBronzeIngestion


def run() -> None:
    logger = get_pipeline_logger("data-pipeline/bronze")
    config_file = Path(__file__).resolve().parents[1] / "configs" / "tables.yaml"
    config = yaml.safe_load(config_file.read_text())

    ingestion = OracleBronzeIngestion()
    for t in config.get("bronze_tables", []):
        if t.get("source", {}).get("type") != "oracle":
            continue
        logger.info(f"Ingerindo {t['source'].get('schema')}.{t['source'].get('table')}")
        ingestion.ingest_table(
            table=t["source"]["table"],
            schema=t["source"].get("schema"),
            partition_column=t.get("ingestion", {}).get("partition_column"),
            num_partitions=int(t.get("ingestion", {}).get("num_partitions", 1)),
            reference_date=t.get("ingestion", {}).get("reference_date"),
        )

