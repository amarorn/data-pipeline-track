import logging
from typing import Optional

_LOGGERS = {}


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
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
    _LOGGERS[name] = logger
    return logger


def get_pipeline_logger(pipeline_name: str, level: Optional[int] = None) -> logging.Logger:
    logger = setup_logger(pipeline_name)
    if level is not None:
        logger.setLevel(level)
    return logger
