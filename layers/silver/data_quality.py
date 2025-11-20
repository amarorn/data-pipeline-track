"""
Pipeline de limpeza e validação de dados para camada Silver
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace, 
    when, coalesce, to_date, to_timestamp
)
from typing import List, Dict, Optional

from shared.infrastructure.storage.storage_manager import StorageManager
from shared.infrastructure.spark.spark_session import SparkSessionManager
from shared.utils.logger import get_pipeline_logger


class DataQuality:
    """Processa dados da Bronze para Silver com qualidade"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session(app_name="Silver-DataQuality")
        self.storage = StorageManager()
        self.logger = get_pipeline_logger("silver", "data_quality")

    def clean_strings(
        self,
        df: DataFrame,
        columns: List[str],
        trim_spaces: bool = True,
        upper_case: bool = False,
        remove_special_chars: bool = False
    ) -> DataFrame:
        """
        Limpa colunas de texto

        Args:
            df: DataFrame
            columns: Lista de colunas
            trim_spaces: Remove espaços
            upper_case: Converte para maiúscula
            remove_special_chars: Remove caracteres especiais
        """
        for column in columns:
            if column in df.columns:
                # Trim
                if trim_spaces:
                    df = df.withColumn(column, trim(col(column)))

                # Upper case
                if upper_case:
                    df = df.withColumn(column, upper(col(column)))

                # Remove caracteres especiais
                if remove_special_chars:
                    df = df.withColumn(
                        column,
                        regexp_replace(col(column), r"[^a-zA-Z0-9\s]", "")
                    )

        return df

    def handle_nulls(
        self,
        df: DataFrame,
        null_replacements: Dict[str, any]
    ) -> DataFrame:
        """
        Trata valores nulos

        Args:
            df: DataFrame
            null_replacements: Dict {coluna: valor_default}
        """
        for column, default_value in null_replacements.items():
            if column in df.columns:
                df = df.withColumn(
                    column,
                    coalesce(col(column), lit(default_value))
                )

        return df

    def remove_duplicates(
        self,
        df: DataFrame,
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> DataFrame:
        """
        Remove duplicatas

        Args:
            df: DataFrame
            subset: Colunas para considerar na deduplicação
            keep: 'first' ou 'last'
        """
        count_before = df.count()

        if subset:
            df = df.dropDuplicates(subset)
        else:
            df = df.dropDuplicates()

        count_after = df.count()
        removed = count_before - count_after

        if removed > 0:
            self.logger.warning(f"Removidas {removed} duplicatas")

        return df

    def validate_schema(
        self,
        df: DataFrame,
        expected_schema: Dict[str, str]
    ) -> DataFrame:
        """
        Valida e ajusta schema

        Args:
            df: DataFrame
            expected_schema: Dict {coluna: tipo}
        """
        for column, dtype in expected_schema.items():
            if column in df.columns:
                df = df.withColumn(column, col(column).cast(dtype))

        return df

    def process_bronze_to_silver(
        self,
        source: str,
        entity: str,
        domain: str,
        transformations: Dict = None
    ) -> DataFrame:
        """
        Processa dados de Bronze para Silver

        Args:
            source: Fonte de dados (ex: oracle)
            entity: Entidade (ex: customers)
            domain: Domínio de negócio (ex: sales)
            transformations: Dict com configurações de transformação
        """
        self.logger.info(f"Processando {source}.{entity} -> {domain}.{entity}")

        # Lê da Bronze
        df = self.storage.read_bronze(source=source, entity=entity)

        if transformations:
            # Limpeza de strings
            if "clean_strings" in transformations:
                df = self.clean_strings(df, **transformations["clean_strings"])

            # Tratamento de nulos
            if "null_replacements" in transformations:
                df = self.handle_nulls(df, transformations["null_replacements"])

            # Remoção de duplicatas
            if "deduplicate" in transformations:
                df = self.remove_duplicates(df, **transformations["deduplicate"])

            # Validação de schema
            if "schema" in transformations:
                df = self.validate_schema(df, transformations["schema"])

        # Salva na Silver
        path = self.storage.write_silver(
            df=df,
            domain=domain,
            entity=entity,
            mode="overwrite"
        )

        count = df.count()
        self.logger.info(f"Processados {count} registros -> {path}")

        return df


if __name__ == "__main__":
    # Exemplo de uso
    dq = DataQuality()

    transformations = {
        "clean_strings": {
            "columns": ["nome", "email"],
            "trim_spaces": True,
            "upper_case": False
        },
        "null_replacements": {
            "status": "ATIVO",
            "tipo": "INDEFINIDO"
        },
        "deduplicate": {
            "subset": ["id"]
        },
        "schema": {
            "id": "integer",
            "data_cadastro": "date"
        }
    }

    dq.process_bronze_to_silver(
        source="oracle",
        entity="tabela_fonte",
        domain="vendas",
        transformations=transformations
    )
