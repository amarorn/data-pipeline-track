"""
Funções utilitárias de hashing para DataFrames Spark.
"""
from __future__ import annotations

from typing import Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_row_hash(
    df: DataFrame,
    *,
    exclude: Optional[Iterable[str]] = None,
    hash_col: str = "row_hash",
) -> DataFrame:
    """Calcula SHA-256 por linha com base no conteúdo completo.

    - Ordena colunas (estável) e exclui metadados opcionais.
    - Usa to_json(struct(...)) para manter tipos/ordem consistentes.
    """
    exclude_set = set(exclude or [])
    cols: List[str] = [c for c in df.columns if c not in exclude_set]
    cols.sort()
    struct_col = F.struct(*(F.col(c) for c in cols))
    return df.withColumn(hash_col, F.sha2(F.to_json(struct_col), 256))

