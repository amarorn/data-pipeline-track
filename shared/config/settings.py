"""
Configurações globais do projeto
"""
from pathlib import Path
from typing import Optional
import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Configurações do projeto"""

    # Paths
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent
    DATA_PATH: Path = PROJECT_ROOT / "data"
    METADATA_PATH: Path = PROJECT_ROOT / "metadata"
    LOGS_PATH: Path = PROJECT_ROOT / "logs"

    # Oracle
    ORACLE_HOST: str = os.getenv("ORACLE_HOST", "localhost")
    ORACLE_PORT: str = os.getenv("ORACLE_PORT", "1521")
    ORACLE_SERVICE: str = os.getenv("ORACLE_SERVICE", "SERVICE_NAME")
    ORACLE_USER: str = os.getenv("ORACLE_USER", "user")
    ORACLE_PASSWORD: str = os.getenv("ORACLE_PASSWORD", "password")

    # ClickHouse
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT: str = os.getenv("CLICKHOUSE_PORT", "9000")
    CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "default")
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_SECURE: bool = os.getenv("CLICKHOUSE_SECURE", "true").lower() == "true"

    # S3/Object Storage (para ClickHouse Cloud)
    S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "")  # Ex: s3.amazonaws.com
    S3_BUCKET: str = os.getenv("S3_BUCKET", "data-lake")
    S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "")
    S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "")
    S3_REGION: str = os.getenv("S3_REGION", "us-east-1")

    # Storage Strategy
    USE_S3_STORAGE: bool = os.getenv("USE_S3_STORAGE", "true").lower() == "true"
    LOCAL_TEMP_DIR: Path = PROJECT_ROOT / "temp"
    AUTO_CLEANUP_TEMP: bool = os.getenv("AUTO_CLEANUP_TEMP", "true").lower() == "true"
    TEMP_RETENTION_HOURS: int = int(os.getenv("TEMP_RETENTION_HOURS", "2"))

    # Spark
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

    # Pipeline
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100000"))
    FETCH_SIZE: int = int(os.getenv("FETCH_SIZE", "10000"))
    NUM_PARTITIONS: int = int(os.getenv("NUM_PARTITIONS", "10"))

    def __post_init__(self):
        """Cria diretórios necessários"""
        self.DATA_PATH.mkdir(parents=True, exist_ok=True)
        self.METADATA_PATH.mkdir(parents=True, exist_ok=True)
        self.LOGS_PATH.mkdir(parents=True, exist_ok=True)

        # Diretório temporário apenas se não usar S3
        if not self.USE_S3_STORAGE:
            self.LOCAL_TEMP_DIR.mkdir(parents=True, exist_ok=True)


# Instância global
settings = Settings()
