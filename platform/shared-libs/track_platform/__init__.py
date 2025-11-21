"""
Track Platform - Bibliotecas compartilhadas para todos os domínios
"""

__version__ = "1.0.0"
__author__ = "Track Data Team"

from shared.infrastructure.spark.spark_session import SparkSessionManager
from .spark import SparkSessionManager
from .storage import StorageManager, ClickHouseStorage
from .connections import OracleConnection, ClickHouseConnection
from .logging import setup_logger, get_pipeline_logger

__all__ = [
    "SparkSessionManager",
    "StorageManager",
    "ClickHouseStorage",
    "OracleConnection",
    "ClickHouseConnection",
    "setup_logger",
    "get_pipeline_logger",
]
