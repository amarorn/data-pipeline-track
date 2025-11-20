"""
Gerenciador de armazenamento no ClickHouse Cloud com S3
"""
from typing import Optional, Dict, List
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import os

from shared.config.settings import settings
from shared.utils.logger import get_pipeline_logger


class ClickHouseStorage:
    """Gerencia armazenamento de dados no ClickHouse Cloud"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_pipeline_logger("storage", "clickhouse")
        self._configure_s3()

    def _configure_s3(self):
        """Configura credenciais S3 no Spark"""
        if settings.USE_S3_STORAGE:
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.access.key", settings.S3_ACCESS_KEY
            )
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.secret.key", settings.S3_SECRET_KEY
            )
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.endpoint", settings.S3_ENDPOINT
            )
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.path.style.access", "true"
            )
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )

    def _get_s3_path(self, layer: str, domain: str, entity: str) -> str:
        """Retorna caminho S3 para dados"""
        return f"s3a://{settings.S3_BUCKET}/{layer}/{domain}/{entity}"

    def _get_clickhouse_table_name(
        self, 
        layer: str, 
        domain: str, 
        entity: str
    ) -> str:
        """Retorna nome da tabela no ClickHouse"""
        return f"{settings.CLICKHOUSE_DB}.{layer}_{domain}_{entity}"

    def write_to_s3_and_clickhouse(
        self,
        df: DataFrame,
        layer: str,
        domain: str,
        entity: str,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        mode: str = "append"
    ):
        """
        Escreve dados em S3 (Parquet) e cria tabela no ClickHouse

        Args:
            df: DataFrame Spark
            layer: bronze, silver ou gold
            domain: Domínio de negócio
            entity: Nome da entidade
            partition_by: Colunas para particionamento
            order_by: Colunas para ordenação (otimização ClickHouse)
            mode: append ou overwrite
        """
        s3_path = self._get_s3_path(layer, domain, entity)
        table_name = self._get_clickhouse_table_name(layer, domain, entity)

        self.logger.info(f"Gravando dados em S3: {s3_path}")

        # Escreve Parquet no S3
        writer = df.write.mode(mode).format("parquet")

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(s3_path)

        self.logger.info(f"Dados gravados em S3: {s3_path}")

        # Cria/atualiza tabela no ClickHouse apontando para S3
        self._create_clickhouse_table_from_s3(
            df=df,
            table_name=table_name,
            s3_path=s3_path,
            partition_by=partition_by,
            order_by=order_by,
            mode=mode
        )

        return s3_path, table_name

    def _create_clickhouse_table_from_s3(
        self,
        df: DataFrame,
        table_name: str,
        s3_path: str,
        partition_by: Optional[List[str]],
        order_by: Optional[List[str]],
        mode: str
    ):
        """Cria tabela no ClickHouse usando S3"""
        from clickhouse_driver import Client

        client = Client(
            host=settings.CLICKHOUSE_HOST,
            port=int(settings.CLICKHOUSE_PORT),
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DB,
            secure=settings.CLICKHOUSE_SECURE
        )

        try:
            # Dropa tabela se mode=overwrite
            if mode == "overwrite":
                self.logger.info(f"Dropando tabela: {table_name}")
                client.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Gera DDL baseado no schema do DataFrame
            columns_ddl = self._generate_clickhouse_columns(df)

            # Define ORDER BY
            order_clause = ""
            if order_by:
                order_clause = f"ORDER BY ({', '.join(order_by)})"
            elif partition_by:
                order_clause = f"ORDER BY ({', '.join(partition_by)})"
            else:
                # Usa primeira coluna como default
                first_col = df.columns[0]
                order_clause = f"ORDER BY {first_col}"

            # Cria tabela com engine S3
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                {columns_ddl}
            )
            ENGINE = S3(
                '{s3_path}/*.parquet',
                '{settings.S3_ACCESS_KEY}',
                '{settings.S3_SECRET_KEY}',
                'Parquet'
            )
            """

            self.logger.info(f"Criando tabela ClickHouse: {table_name}")
            client.execute(create_table_sql)

            # Verifica contagem
            count = client.execute(f"SELECT count() FROM {table_name}")[0][0]
            self.logger.info(f"Tabela {table_name} criada com {count} registros")

        except Exception as e:
            self.logger.error(f"Erro ao criar tabela ClickHouse: {str(e)}")
            raise
        finally:
            client.disconnect()

    def _generate_clickhouse_columns(self, df: DataFrame) -> str:
        """Gera DDL de colunas para ClickHouse baseado no schema Spark"""
        type_mapping = {
            "string": "String",
            "integer": "Int32",
            "long": "Int64",
            "double": "Float64",
            "float": "Float32",
            "boolean": "UInt8",
            "date": "Date",
            "timestamp": "DateTime",
            "decimal": "Decimal(18,2)"
        }

        columns = []
        for field in df.schema.fields:
            spark_type = field.dataType.simpleString().lower()

            # Mapeia tipo Spark para ClickHouse
            ch_type = type_mapping.get(spark_type, "String")

            # Nullable
            if field.nullable:
                ch_type = f"Nullable({ch_type})"

            columns.append(f"{field.name} {ch_type}")

        return ",\n    ".join(columns)

    def write_directly_to_clickhouse(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        batch_size: int = 100000
    ):
        """
        Escreve dados diretamente em tabela ClickHouse (sem S3)
        Útil para tabelas pequenas ou temporárias

        Args:
            df: DataFrame Spark
            table_name: Nome completo da tabela (db.table)
            mode: append ou overwrite
            batch_size: Tamanho do batch
        """
        self.logger.info(f"Gravando dados diretamente no ClickHouse: {table_name}")

        jdbc_url = f"jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}/{settings.CLICKHOUSE_DB}"

        if settings.CLICKHOUSE_SECURE:
            jdbc_url += "?ssl=true"

        properties = {
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "user": settings.CLICKHOUSE_USER,
            "password": settings.CLICKHOUSE_PASSWORD,
            "batchsize": str(batch_size)
        }

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .options(**properties) \
            .mode(mode) \
            .save()

        self.logger.info(f"Dados gravados no ClickHouse: {table_name}")

    def read_from_clickhouse(
        self,
        table_name: str,
        filters: Optional[str] = None
    ) -> DataFrame:
        """
        Lê dados de tabela ClickHouse

        Args:
            table_name: Nome completo da tabela
            filters: Cláusula WHERE (opcional)
        """
        query = f"SELECT * FROM {table_name}"
        if filters:
            query += f" WHERE {filters}"

        jdbc_url = f"jdbc:clickhouse://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}/{settings.CLICKHOUSE_DB}"

        if settings.CLICKHOUSE_SECURE:
            jdbc_url += "?ssl=true"

        properties = {
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "user": settings.CLICKHOUSE_USER,
            "password": settings.CLICKHOUSE_PASSWORD
        }

        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .options(**properties) \
            .load()

        return df
