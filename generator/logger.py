import logging
import sys
from typing import Optional

LOG_FORMAT = '%(asctime)s - %(filename)s - %(funcName)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_logger(name: Optional[str] = None, level: int = logging.DEBUG) -> logging.Logger:
    """
    Set up a _logger that will log only to the console (stdout).
    """
    _logger = logging.getLogger(name or __name__)
    _logger.setLevel(level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    console_handler.setFormatter(console_formatter)

    # Add console handler to the _logger
    _logger.addHandler(console_handler)

    return _logger


__all__ = ['get_logger']



