"""
Track Platform - Bibliotecas compartilhadas para todos os domínios
"""

__version__ = "1.0.0"
__author__ = "Track Data Team"

from .spark.session_manager import SparkSessionManager
from .logging import setup_logger, get_pipeline_logger
from .connections import OracleConnection
from .storage import StorageManager

__all__ = [
    "SparkSessionManager",
    "setup_logger",
    "get_pipeline_logger",
    "OracleConnection",
    "StorageManager",
]
