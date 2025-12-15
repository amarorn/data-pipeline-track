"""
Runner da camada Silver para o domínio data-pipeline.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from track_platform.spark import SparkSessionManager
from track_platform.logging import get_pipeline_logger
from track_platform.storage import StorageManager
from .transformations import SilverProcessor


def run() -> None:
    logger = get_pipeline_logger("data-pipeline/silver")
    spark = SparkSessionManager.get_session(app_name="Track-Silver-Runner")
    storage = StorageManager()

    # Entrada: bronze/oracle/* do dia de referência (D-1). Para simplicidade, lê tudo e processa.
    # Em ambiente real, filtrar por reference_date específico.
    input_path = f"{storage.base_path}/bronze/oracle/*"
    logger.info(f"Lendo bronze de: {input_path}")
    df = spark.read.parquet(input_path)

    processor = SilverProcessor()
    df_silver = processor.process(
        df,
        reference_date_col="reference_date",
        exclude_from_hash={"_ingestion_timestamp", "_source", "_source_table"},
        source="oracle",
        entity="consolidated",
    )

    logger.info(f"Registros na silver: {df_silver.count()}")

