"""
Gerenciador de armazenamento para camadas de dados
Suporta armazenamento local (fallback) e ClickHouse Cloud com S3
"""
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
import os
import shutil

from shared.config.settings import settings


class StorageManager:
    """Gerencia armazenamento de dados nas camadas"""

    def __init__(self, base_path: Optional[str] = None, spark: Optional[SparkSession] = None):
        self.use_clickhouse = settings.USE_S3_STORAGE
        self.base_path = Path(base_path or settings.LOCAL_TEMP_DIR)

        if self.use_clickhouse:
            from shared.infrastructure.storage.clickhouse_storage import ClickHouseStorage
            if spark is None:
                from shared.infrastructure.spark.spark_session import SparkSessionManager
                spark = SparkSessionManager.get_session()
            self.clickhouse_storage = ClickHouseStorage(spark)
        else:
            self._ensure_directories()

    def _ensure_directories(self):
        """Garante que os diretórios existem (apenas para modo local)"""
        for layer in ["bronze", "silver", "gold"]:
            (self.base_path / layer).mkdir(parents=True, exist_ok=True)

    def cleanup_temp_files(self, hours: Optional[int] = None):
        """
        Remove arquivos temporários antigos

        Args:
            hours: Arquivos mais antigos que X horas serão removidos
        """
        if not settings.AUTO_CLEANUP_TEMP:
            return

        hours = hours or settings.TEMP_RETENTION_HOURS
        cutoff_time = datetime.now() - timedelta(hours=hours)

        if self.base_path.exists():
            for item in self.base_path.rglob("*"):
                if item.is_file():
                    file_time = datetime.fromtimestamp(item.stat().st_mtime)
                    if file_time < cutoff_time:
                        try:
                            item.unlink()
                        except Exception:
                            pass

    def get_layer_path(self, layer: str, source: str, entity: str) -> Path:
        """
        Retorna caminho para uma entidade em uma camada

        Args:
            layer: bronze, silver ou gold
            source: Nome da fonte (ex: oracle, api)
            entity: Nome da entidade (ex: customers, orders)
        """
        return self.base_path / layer / source / entity

    def write_bronze(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        partition_by: Optional[list] = None,
        mode: str = "append"
    ):
        """
        Escreve dados na camada Bronze

        Args:
            df: DataFrame Spark
            source: Nome da fonte
            entity: Nome da entidade
            partition_by: Colunas de particionamento
            mode: Modo de escrita
        """
        from pyspark.sql.functions import lit, current_timestamp

        # Adiciona metadados de ingestão
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_ingestion_date", 
                          lit(datetime.now().strftime("%Y-%m-%d")))

        if self.use_clickhouse:
            # Grava em S3 + ClickHouse
            s3_path, table_name = self.clickhouse_storage.write_to_s3_and_clickhouse(
                df=df,
                layer="bronze",
                domain=source,
                entity=entity,
                partition_by=partition_by or ["_ingestion_date"],
                mode=mode
            )

            # Cleanup de arquivos temporários
            self.cleanup_temp_files()

            return s3_path
        else:
            # Fallback: grava localmente
            path = self.get_layer_path("bronze", source, entity)

            writer = df.write.mode(mode).format("parquet")

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(str(path))

            return path

    def read_bronze(
        self,
        source: str,
        entity: str,
        date_filter: Optional[str] = None
    ) -> DataFrame:
        """
        Lê dados da camada Bronze

        Args:
            source: Nome da fonte
            entity: Nome da entidade
            date_filter: Filtro de data (formato: YYYY-MM-DD)
        """
        if self.use_clickhouse:
            # Lê do ClickHouse
            table_name = self.clickhouse_storage._get_clickhouse_table_name(
                "bronze", source, entity
            )

            filters = None
            if date_filter:
                filters = f"_ingestion_date = '{date_filter}'"

            return self.clickhouse_storage.read_from_clickhouse(
                table_name=table_name,
                filters=filters
            )
        else:
            # Fallback: lê localmente
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

            path = self.get_layer_path("bronze", source, entity)

            if not path.exists():
                raise FileNotFoundError(f"Caminho não encontrado: {path}")

            df = spark.read.format("parquet").load(str(path))

            if date_filter and "_ingestion_date" in df.columns:
                df = df.filter(f"_ingestion_date = '{date_filter}'")

            return df

    def write_silver(
        self,
        df: DataFrame,
        domain: str,
        entity: str,
        partition_by: Optional[list] = None,
        mode: str = "overwrite"
    ):
        """
        Escreve dados na camada Silver

        Args:
            df: DataFrame Spark
            domain: Domínio de negócio
            entity: Nome da entidade
            partition_by: Colunas de particionamento
            mode: Modo de escrita
        """
        from pyspark.sql.functions import current_timestamp

        # Adiciona metadados de processamento
        df = df.withColumn("_processed_timestamp", current_timestamp())

        if self.use_clickhouse:
            s3_path, table_name = self.clickhouse_storage.write_to_s3_and_clickhouse(
                df=df,
                layer="silver",
                domain=domain,
                entity=entity,
                partition_by=partition_by,
                order_by=partition_by,  # Otimização ClickHouse
                mode=mode
            )

            self.cleanup_temp_files()
            return s3_path
        else:
            path = self.base_path / "silver" / domain / entity

            writer = df.write.mode(mode).format("parquet")

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(str(path))

            return path

    def read_silver(self, domain: str, entity: str) -> DataFrame:
        """Lê dados da camada Silver"""
        if self.use_clickhouse:
            table_name = self.clickhouse_storage._get_clickhouse_table_name(
                "silver", domain, entity
            )
            return self.clickhouse_storage.read_from_clickhouse(table_name)
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

            path = self.base_path / "silver" / domain / entity

            if not path.exists():
                raise FileNotFoundError(f"Caminho não encontrado: {path}")

            return spark.read.format("parquet").load(str(path))

    def write_gold(
        self,
        df: DataFrame,
        domain: str,
        entity: str,
        mode: str = "overwrite"
    ):
        """
        Escreve dados na camada Gold

        Args:
            df: DataFrame Spark
            domain: Domínio de negócio
            entity: Nome da entidade/métrica
            mode: Modo de escrita
        """
        from pyspark.sql.functions import current_timestamp

        # Adiciona metadados de agregação
        df = df.withColumn("_aggregation_timestamp", current_timestamp())

        if self.use_clickhouse:
            # Para Gold, usa escrita direta no ClickHouse (mais eficiente para métricas)
            table_name = f"{settings.CLICKHOUSE_DB}.gold_{domain}_{entity}"

            self.clickhouse_storage.write_directly_to_clickhouse(
                df=df,
                table_name=table_name,
                mode=mode
            )

            self.cleanup_temp_files()
            return table_name
        else:
            path = self.base_path / "gold" / domain / entity
            df.write.mode(mode).format("parquet").save(str(path))
            return path

    def read_gold(self, domain: str, entity: str) -> DataFrame:
        """Lê dados da camada Gold"""
        if self.use_clickhouse:
            table_name = f"{settings.CLICKHOUSE_DB}.gold_{domain}_{entity}"
            return self.clickhouse_storage.read_from_clickhouse(table_name)
        else:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

            path = self.base_path / "gold" / domain / entity

            if not path.exists():
                raise FileNotFoundError(f"Caminho não encontrado: {path}")

            return spark.read.format("parquet").load(str(path))
