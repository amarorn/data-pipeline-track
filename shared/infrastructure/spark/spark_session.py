"""
Gerenciador de Sessão Spark
"""
from pyspark.sql import SparkSession
from typing import Optional, Dict
import os


class SparkSessionManager:
    """Gerencia criação e configuração de SparkSession"""

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(
        cls,
        app_name: str = "DataPipeline",
        config: Optional[Dict[str, str]] = None,
        master: Optional[str] = None
    ) -> SparkSession:
        """
        Retorna ou cria uma SparkSession

        Args:
            app_name: Nome da aplicação
            config: Configurações adicionais
            master: URL do master (local, yarn, etc)
        """
        if cls._instance is None:
            builder = SparkSession.builder.appName(app_name)

            # Master
            if master:
                builder = builder.master(master)
            elif os.getenv("SPARK_MASTER"):
                builder = builder.master(os.getenv("SPARK_MASTER"))
            else:
                builder = builder.master("local[*]")

            # Configurações padrão
            default_config = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.parquet.compression.codec": "snappy",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.sql.session.timeZone": "America/Sao_Paulo",
                # Configurações S3
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                "spark.hadoop.fs.s3a.path.style.access": "false",
                "spark.hadoop.fs.s3a.fast.upload": "true",
                # Buffer em memória para minimizar disco local
                "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
                "spark.hadoop.fs.s3a.multipart.size": "104857600",  # 100MB
            }

            # Merge com configurações customizadas
            if config:
                default_config.update(config)

            # Aplica configurações
            for key, value in default_config.items():
                builder = builder.config(key, value)

            cls._instance = builder.getOrCreate()

            # Define nível de log
            cls._instance.sparkContext.setLogLevel("WARN")

        return cls._instance

    @classmethod
    def stop_session(cls):
        """Para a SparkSession"""
        if cls._instance:
            cls._instance.stop()
            cls._instance = None
