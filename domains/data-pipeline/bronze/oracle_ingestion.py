"""
Pipeline de ingestão de dados do Oracle para camada Bronze
Domínio: data-pipeline
"""
from datetime import datetime, timedelta
from typing import Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

# Imports das bibliotecas compartilhadas da plataforma
from track_platform.connections import OracleConnection
from track_platform.storage import StorageManager
from track_platform.spark import SparkSessionManager
from track_platform.logging import get_pipeline_logger


class OracleBronzeIngestion:
    """Ingesta dados do Oracle para camada Bronze"""

    def __init__(self):
        self.spark = SparkSessionManager.get_session(app_name="Track-Bronze-Oracle")
        self.oracle = OracleConnection(self.spark)
        self.storage = StorageManager()
        self.logger = get_pipeline_logger("bronze", "oracle")

    def ingest_table(
        self,
        table: str,
        schema: Optional[str] = None,
        partition_column: Optional[str] = None,
        num_partitions: int = 10,
        reference_date: Optional[str] = None,
    ) -> DataFrame:
        """
        Ingere tabela do Oracle

        Args:
            table: Nome da tabela
            schema: Schema da tabela
            partition_column: Coluna para paralelização
            num_partitions: Número de partições
            incremental_column: Coluna para carga incremental
            last_value: Último valor processado
        """
        self.logger.info(f"Iniciando ingestão: {schema}.{table}")

        try:
            # Leitura do Oracle
            df = self.oracle.read_table(
                table=table,
                schema=schema,
                partition_column=partition_column,
                num_partitions=num_partitions
            )

            # Define reference_date (D-1 por padrão)
            if reference_date is None:
                ref = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                ref = reference_date

            # Adiciona metadados de ingestão
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            df = df.withColumn("reference_date", lit(ref))
            df = df.withColumn("_source", lit("oracle"))
            df = df.withColumn("_source_table", 
                             lit(f"{schema}.{table}" if schema else table))

            # Contagem
            count = df.count()
            self.logger.info(f"Registros lidos: {count}")

            # Salva na camada Bronze
            path = self.storage.write_bronze(
                df=df,
                source="oracle",
                entity=table.lower(),
                partition_by=["reference_date"],
                mode="append"
            )

            self.logger.info(f"Dados salvos em: {path}")

            return df

        except Exception as e:
            self.logger.error(f"Erro na ingestão: {str(e)}")
            raise

    def ingest_multiple_tables(
        self,
        tables: List[dict]
    ):
        """
        Ingere múltiplas tabelas

        Args:
            tables: Lista de dicts com configurações de cada tabela
                   Ex: [{"table": "customers", "schema": "sales", ...}]
        """
        for table_config in tables:
            try:
                self.ingest_table(**table_config)
            except Exception as e:
                self.logger.error(
                    f"Erro ao ingerir {table_config.get('table')}: {str(e)}"
                )
                continue


if __name__ == "__main__":
    # Exemplo de uso
    ingestion = OracleBronzeIngestion()

    # Ingere uma tabela
    ingestion.ingest_table(
        table="TABELA_FONTE",
        schema="SCHEMA",
        partition_column="ID",
        num_partitions=10
    )
