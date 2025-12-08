"""
tasks.py
Contains ETL task-level functions for DS206 Project 2.
These tasks execute parametrized SQL scripts in the correct sequence.
Each task returns {'success': True/False} and accepts prerequisites.
"""

import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils import (
    load_db_config,
    create_db_connection,
    read_sql_file,
    execute_sql_with_params
)

# --------------------------------------------------------
# Helper: run a parametrized SQL file
# --------------------------------------------------------
def run_sql_task(sql_path: str, params: dict) -> dict:
    """
    Execute a parametrized SQL script inside a database transaction.
    Automatically splits SQL batches separated by 'GO'.
    """

    try:
        cfg = load_db_config("sql_server_config.cfg")
        conn = create_db_connection(cfg)
        cursor = conn.cursor()

        sql_text = read_sql_file(sql_path)

        # Replace parameters
        if params:
            for key, value in params.items():
                sql_text = sql_text.replace(f"@{key}", str(value))

        # Split batches by GO (case-insensitive, standalone)
        batches = [
            batch.strip()
            for batch in sql_text.split("\nGO")
            if batch.strip()
        ]

        for batch in batches:
            cursor.execute(batch)
            conn.commit()

        conn.close()
        return {"success": True}

    except Exception as e:
        return {"success": False, "message": str(e)}


def task_create_dimensional_tables() -> dict:
    """
    Create all dimensional tables using the predefined SQL script.

    This task executes the dimensional_db_table_creation.sql file,
    which contains CREATE TABLE statements for all dimensions and fact tables.

    Returns
    -------
    dict
        {"success": True} if tables were created successfully,
        otherwise {"success": False, "message": "..."}.
    """
    sql_path = os.path.join(
        PROJECT_ROOT,
        "infrastructure_initiation",
        "dimensional_db_table_creation.sql"
    )

    params = {
        "database": "ORDER_DDS",
        "schema": "dbo"
    }

    return run_sql_task(sql_path, params)

def task_update_dim_categories(prerequisite: dict = None) -> dict:
    """
    Load data into Dim_Categories from staging_Categories.

    Parameters
    ----------
    prerequisite : dict
        Output of the previous task. If provided and not successful,
        this task will not run.

    Returns
    -------
    dict
        {"success": True} if ingestion completed successfully,
        otherwise {"success": False, "message": "..."}.
    """

    # Check prerequisite
    if prerequisite and not prerequisite.get("success", False):
        return {"success": False, "message": "Prerequisite task failed"}

    # SQL file path
    sql_path = os.path.join(
        PROJECT_ROOT,
        "pipeline_dimensional_data",
        "queries",
        "update_dim_categories.sql"
    )
    # Parameters used inside SQL script
    params = {
        "database_name": "ORDER_DDS",
        "schema_name": "dbo",
        "dim_table_name": "DimCategories",
        "staging_table_name": "staging_Categories"
            }

    return run_sql_task(sql_path, params)
