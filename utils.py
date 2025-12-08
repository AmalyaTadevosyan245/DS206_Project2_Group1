"""
utils.py

Utility functions for DS206 Project 2 ETL Pipeline.

Supports:
- Reading SQL files from disk
- Replacing T-SQL style template parameters (@param)
- Loading SQL Server config
- Creating DB connections via pyodbc
- Executing parameterized SQL scripts
- Generating UUIDs & timestamps
"""

import os
import uuid
import datetime
import pyodbc
import configparser


# ==============================================================
# 1. Read SQL file
# ==============================================================

def read_sql_file(filepath: str) -> str:
    """Return SQL script content from a file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"SQL file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


# ==============================================================
# 2. Replace @variables inside SQL script
# ==============================================================

def render_sql(sql_text: str, params: dict) -> str:
    """
    Replace T-SQL template parameters (@param_name) with actual values.

    Example:
        @database_name → DS206_DB
        @schema_name → dbo
    """
    for key, value in params.items():
        sql_text = sql_text.replace(f"@{key}", str(value))

    return sql_text


# ==============================================================
# 3. Load DB config file
# ==============================================================

def load_db_config(cfg_path: str = "sql_server_config.cfg") -> dict:
    """Load config from the project root regardless of where Python is executed."""

    # Directory of this utils.py file
    utils_dir = os.path.dirname(os.path.abspath(__file__))

    project_root = utils_dir  

    full_path = os.path.join(project_root, cfg_path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file not found: {full_path}")

    config = configparser.ConfigParser()
    config.read(full_path)

    db_cfg = config["sql_server"]

    return {
        "server": db_cfg.get("server"),
        "database": db_cfg.get("database"),
        "username": db_cfg.get("username", ""),
        "password": db_cfg.get("password", ""),
        "driver": db_cfg.get("driver", "ODBC Driver 18 for SQL Server"),
        "trusted_connection": db_cfg.get("trusted_connection", "no"),
        "encrypt": db_cfg.get("encrypt", "no"),
        "trust_server_certificate": db_cfg.get("trust_server_certificate", "yes")
    }



# ==============================================================
# 4. Create SQL connection (pyodbc)
# ==============================================================

def create_db_connection(cfg: dict):
    """Return a live SQL Server connection."""

    # Build connection string
    if cfg["trusted_connection"].lower() == "yes":
        conn_str = (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"Trusted_Connection=yes;"
            f"Encrypt={cfg['encrypt']};"
            f"TrustServerCertificate={cfg['trust_server_certificate']};"
        )
    else:
        conn_str = (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            f"UID={cfg['username']};"
            f"PWD={cfg['password']};"
            f"Encrypt={cfg['encrypt']};"
            f"TrustServerCertificate={cfg['trust_server_certificate']};"
        )

    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        raise ConnectionError(f"Failed SQL connection: {e}")

# ==============================================================
# 5. Execute SQL (with replaced params)
# ==============================================================

def execute_sql(conn, sql_text: str):
    """Execute SQL using pyodbc."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql_text)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"SQL execution failed: {e}")


# ==============================================================
# 6. UUID + timestamp helpers
# ==============================================================

def get_uuid() -> str:
    return str(uuid.uuid4())

def get_timestamp() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")