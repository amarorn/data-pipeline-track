"""
Configuração de logging
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Configura e retorna um logger

    Args:
        name: Nome do logger
        log_file: Caminho para arquivo de log
        level: Nível de logging
        format_string: Formato customizado
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove handlers existentes
    logger.handlers.clear()

    # Formato
    if format_string is None:
        format_string = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(message)s"
        )

    formatter = logging.Formatter(format_string)

    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler para arquivo
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_pipeline_logger(layer: str, entity: str) -> logging.Logger:
    """
    Retorna logger para pipeline específico

    Args:
        layer: Camada (bronze, silver, gold)
        entity: Nome da entidade
    """
    from shared.config.settings import settings

    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = settings.LOGS_PATH / layer / f"{entity}_{timestamp}.log"

    return setup_logger(
        name=f"{layer}.{entity}",
        log_file=log_file
    )
