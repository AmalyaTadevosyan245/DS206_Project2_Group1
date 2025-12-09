"""
flow.py
Coordinates the execution of all dimensional ETL tasks (strict sequence).

Class:
    DimensionalDataFlow

Responsibilities:
    - Generate execution_id (uuid)
    - Run tasks sequentially
    - Handle start_date / end_date
    - Log each step
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils import get_uuid, get_timestamp
from logging import getLogger
import logging

# Import all tasks
from pipeline_dimensional_data.tasks import (
    task_populate_staging,
    task_create_dimensional_tables,
    task_update_dim_categories,
    task_update_dim_customers,
    task_update_dim_employees,
    task_update_dim_products,
    task_update_dim_region,
    task_update_dim_shippers,
    task_update_dim_suppliers,
    task_update_dim_territories,
    task_update_factorders,
    task_update_fact_error
)


# ==============================================================
# Configure pipeline logger
# ==============================================================

logger = logging.getLogger("dimensional_data_flow")
logger.setLevel(logging.INFO)

log_file_path = os.path.join(
    PROJECT_ROOT, "logs", "logs_dimensional_data_pipeline.txt"
)

file_handler = logging.FileHandler(log_file_path)
formatter = logging.Formatter(
    "%(asctime)s | execution_id=%(execution_id)s | %(levelname)s | %(message)s"
)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


# ==============================================================
# DimensionalDataFlow Class
# ==============================================================

class DimensionalDataFlow:

    def __init__(self):
        """
        Constructor: creates a unique execution_id
        """
        self.execution_id = get_uuid()

        # Inject execution_id into logger
        for handler in logger.handlers:
            handler.addFilter(self._ContextFilter(self.execution_id))

        logger.info("Pipeline initialized")

    # ----------------------------------------------------------
    # Custom log context filter to attach execution_id
    # ----------------------------------------------------------
    class _ContextFilter(logging.Filter):
        def __init__(self, execution_id):
            super().__init__()
            self.execution_id = execution_id

        def filter(self, record):
            record.execution_id = self.execution_id
            return True

    # ----------------------------------------------------------
    # Execute a task and log its result.
    # ----------------------------------------------------------
    def _run_task(self, task_fn, prereq=None, **kwargs):
        if prereq and not prereq.get("success", False):
            msg = f"Task skipped due to failed prerequisite: {task_fn.__name__}"
            logger.error(msg)
            return {"success": False, "message": msg}

        logger.info(f"Starting task: {task_fn.__name__}")

        if not kwargs and prereq is None:
            result = task_fn()
        elif prereq is None:
            result = task_fn(**kwargs)
        else:
            result = task_fn(prereq=prereq, **kwargs)

        if result.get("success"):
            logger.info(f"Task succeeded: {task_fn.__name__}")
        else:
            logger.error(
                f"Task failed: {task_fn.__name__} | {result.get('message')}"
            )

        return result


def exec(self, start_date, end_date):
    logger.info(f"Pipeline execution started | start_date={start_date} end_date={end_date}")

    # 0. Populate staging tables first
    t0 = self._run_task(task_populate_staging)

    # 1. Create all dimensional tables
    t1 = self._run_task(task_create_dimensional_tables, prereq=t0)

    # 2. Load SCD1/SCD2 dimensions
    t2 = self._run_task(task_update_dim_categories, prereq=t1)
    t3 = self._run_task(task_update_dim_customers, prereq=t2)
    t4 = self._run_task(task_update_dim_employees, prereq=t3)
    t5 = self._run_task(task_update_dim_products, prereq=t4)
    t6 = self._run_task(task_update_dim_region, prereq=t5)
    t7 = self._run_task(task_update_dim_shippers, prereq=t6)
    t8 = self._run_task(task_update_dim_suppliers, prereq=t7)
    t9 = self._run_task(task_update_dim_territories, prereq=t8)

    # 3. Load snapshot fact table
    t_fact = self._run_task(
        task_update_factorders,
        prereq=t9,
        start_date=start_date,
        end_date=end_date
    )

    # 4. Load fact_error table
    t_error = self._run_task(
        task_update_fact_error,
        prereq=t_fact,
        start_date=start_date,
        end_date=end_date
    )

    final_success = t_error.get("success", False)
    if final_success:
        logger.info("Pipeline completed successfully.")
    else:
        logger.error("Pipeline failed before completion.")

    return {"success": final_success, "execution_id": self.execution_id}
