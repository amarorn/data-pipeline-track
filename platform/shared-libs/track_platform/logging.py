import logging
from pathlib import Path
from typing import Optional

_LOGGERS = {}


def setup_logger(name: str, level: int = logging.INFO, log_file: Optional[Path] = None) -> logging.Logger:
    logger = _LOGGERS.get(name)
    if logger is not None:
        return logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        if log_file is not None:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    _LOGGERS[name] = logger
    return logger


def get_pipeline_logger(pipeline_name: str, level: Optional[int] = None) -> logging.Logger:
    logger = setup_logger(pipeline_name)
    if level is not None:
        logger.setLevel(level)
    return logger
