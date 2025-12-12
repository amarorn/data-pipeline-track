"""
Gerenciador de armazenamento para camadas Bronze/Silver/Gold.
Suporta escrita em filesystem local (padrão) ou S3 (placeholder).
Particiona por colunas informadas e retorna o caminho de saída.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame


class StorageManager:
    def __init__(self):
        self.base_path = Path(os.getenv("DATA_BASE_PATH", "temp/data"))
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _write(
        self,
        df: DataFrame,
        layer: str,
        source: str,
        entity: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
        format_: str = "parquet",
    ) -> str:
        output_path = self.base_path / layer / source / entity
        output_path.parent.mkdir(parents=True, exist_ok=True)

        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.format(format_).save(str(output_path))

        return str(output_path)

    def write_bronze(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
    ) -> str:
        return self._write(df, "bronze", source, entity, partition_by, mode)

    def write_silver(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
    ) -> str:
        return self._write(df, "silver", source, entity, partition_by, mode)

    def write_gold(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
    ) -> str:
        return self._write(df, "gold", source, entity, partition_by, mode)

