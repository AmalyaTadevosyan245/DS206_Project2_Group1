"""
logging.py

Configures the logger used by DimensionalDataFlow.
Logs are written to logs/logs_dimensional_data_pipeline.txt
Each log entry includes execution_id (injected by flow.py).
"""

import logging
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(PROJECT_ROOT, "logs", "logs_dimensional_data_pipeline.txt")

def get_pipeline_logger():
    """
    Create and return a logger for the dimensional data pipeline.
    flow.py will attach an execution_id filter at runtime.
    """
    logger = logging.getLogger("dimensional_data_flow")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_PATH)
        formatter = logging.Formatter(
            "%(asctime)s | execution_id=%(execution_id)s | %(levelname)s | %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger