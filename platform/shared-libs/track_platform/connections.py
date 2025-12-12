"""
Conexões de dados - Track Platform

Implementa OracleConnection para leitura via Spark JDBC.
Depende de variáveis de ambiente ORACLE_* para compor a URL.
"""
from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


class OracleConnection:
    """Wrapper simples para leitura de tabelas Oracle via Spark JDBC.

    Observações:
    - Requer o driver JDBC do Oracle disponível no classpath do Spark.
    - Variáveis de ambiente necessárias: ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE,
      ORACLE_USER, ORACLE_PASSWORD.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.host = os.getenv("ORACLE_HOST", "localhost")
        self.port = os.getenv("ORACLE_PORT", "1521")
        self.service = os.getenv("ORACLE_SERVICE", "ORCLPDB1")
        self.user = os.getenv("ORACLE_USER", "user")
        self.password = os.getenv("ORACLE_PASSWORD", "password")

    def _jdbc_url(self) -> str:
        # Formato EZConnect
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service}"

    def read_table(
        self,
        table: str,
        schema: Optional[str] = None,
        partition_column: Optional[str] = None,
        num_partitions: int = 1,
        fetch_size: int = 10000,
    ) -> DataFrame:
        """Lê uma tabela Oracle como DataFrame.

        Args:
            table: Nome da tabela (ex.: CUSTOMERS)
            schema: Schema (ex.: SALES)
            partition_column: Coluna para paralelização
            num_partitions: Número de partições
            fetch_size: Tamanho do fetch JDBC
        """
        full_table = f"{schema}.{table}" if schema else table

        reader = (
            self.spark.read.format("jdbc")
            .option("url", self._jdbc_url())
            .option("dbtable", full_table)
            .option("user", self.user)
            .option("password", self.password)
            .option("fetchsize", fetch_size)
            .option("driver", "oracle.jdbc.OracleDriver")
        )

        if partition_column and num_partitions and num_partitions > 1:
            # Para paralelizar, Spark requer bounds; quando não disponíveis,
            # usa-se numPartitions=1 para leitura serial.
            reader = reader.option("partitionColumn", partition_column).option(
                "numPartitions", num_partitions
            )

        return reader.load()

