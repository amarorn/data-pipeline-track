"""
Gerenciador de conexões com bancos de dados
"""
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import os


class DatabaseConnection:
    """Classe base para conexões de banco de dados"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_jdbc_options(self) -> Dict[str, str]:
        """Retorna opções JDBC base"""
        raise NotImplementedError


class OracleConnection(DatabaseConnection):
    """Conexão com Oracle Database"""

    def __init__(
        self,
        spark: SparkSession,
        host: Optional[str] = None,
        port: Optional[str] = None,
        service_name: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        super().__init__(spark)
        self.host = host or os.getenv("ORACLE_HOST")
        self.port = port or os.getenv("ORACLE_PORT", "1521")
        self.service_name = service_name or os.getenv("ORACLE_SERVICE")
        self.user = user or os.getenv("ORACLE_USER")
        self.password = password or os.getenv("ORACLE_PASSWORD")

    def get_jdbc_options(self) -> Dict[str, str]:
        """Retorna opções JDBC para Oracle"""
        return {
            "driver": "oracle.jdbc.OracleDriver",
            "url": f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}",
            "user": self.user,
            "password": self.password
        }

    def read_table(
        self,
        table: str,
        schema: Optional[str] = None,
        fetch_size: int = 10000,
        partition_column: Optional[str] = None,
        num_partitions: Optional[int] = None,
        predicates: Optional[list] = None
    ):
        """
        Lê tabela do Oracle com otimizações

        Args:
            table: Nome da tabela
            schema: Schema da tabela
            fetch_size: Tamanho do fetch
            partition_column: Coluna para particionamento
            num_partitions: Número de partições
            predicates: Lista de predicados para particionamento manual
        """
        options = self.get_jdbc_options()

        if schema:
            table = f"{schema}.{table}"

        options["dbtable"] = table
        options["fetchsize"] = str(fetch_size)

        reader = self.spark.read.format("jdbc")

        for key, value in options.items():
            reader = reader.option(key, value)

        # Paralelização
        if predicates:
            reader = reader.option("predicates", predicates)
        elif partition_column and num_partitions:
            reader = reader.option("partitionColumn", partition_column)
            reader = reader.option("numPartitions", str(num_partitions))

        return reader.load()


class ClickHouseConnection(DatabaseConnection):
    """Conexão com ClickHouse"""

    def __init__(
        self,
        spark: SparkSession,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None
    ):
        super().__init__(spark)
        self.host = host or os.getenv("CLICKHOUSE_HOST")
        self.port = port or os.getenv("CLICKHOUSE_PORT", "9000")
        self.database = database or os.getenv("CLICKHOUSE_DB", "default")

    def get_jdbc_options(self) -> Dict[str, str]:
        """Retorna opções JDBC para ClickHouse"""
        return {
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "url": f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}",
        }

    def write_table(
        self,
        df,
        table: str,
        mode: str = "append",
        batch_size: int = 100000
    ):
        """
        Escreve dados no ClickHouse

        Args:
            df: DataFrame Spark
            table: Nome da tabela
            mode: Modo de escrita (append, overwrite)
            batch_size: Tamanho do batch
        """
        options = self.get_jdbc_options()
        options["dbtable"] = table
        options["batchsize"] = str(batch_size)

        writer = df.write.format("jdbc").mode(mode)

        for key, value in options.items():
            writer = writer.option(key, value)

        writer.save()
