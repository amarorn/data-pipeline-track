"""
Camada Silver - limpeza, normalização, hashing e deduplicação.
"""
from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from track_platform.utils import add_row_hash
from track_platform.logging import get_pipeline_logger
from track_platform.spark import SparkSessionManager
from track_platform.storage import StorageManager


def clean_strings(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def deduplicate_by_hash(df: DataFrame, hash_col: str = "row_hash") -> DataFrame:
    return df.dropDuplicates([hash_col])


class SilverProcessor:
    def __init__(self):
        self.logger = get_pipeline_logger("silver")
        self.spark = SparkSessionManager.get_session(app_name="Track-Silver")
        self.storage = StorageManager()

    def process(
        self,
        df: DataFrame,
        *,
        reference_date_col: str = "reference_date",
        exclude_from_hash: Optional[Iterable[str]] = None,
        source: str = "oracle",
        entity: str = "",
    ) -> DataFrame:
        self.logger.info("Normalizando strings e calculando row_hash")
        df = add_row_hash(df, exclude=exclude_from_hash or {reference_date_col})
        df = deduplicate_by_hash(df)

        path = self.storage.write_silver(
            df=df,
            source=source,
            entity=entity,
            partition_by=[reference_date_col],
            mode="append",
        )
        self.logger.info(f"Silver salvo em: {path}")
        return df

