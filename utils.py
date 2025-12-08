"""
utils.py
Flow-agnostic utility functions for DS206 Project 2 ETL Pipeline.

Provides:
- read_sql_file: loads .sql script content.
- load_db_config: parses sql_server_config.cfg.
- create_db_connection: returns a live SQL Server connection.
- execute_sql_with_params: runs parametrized SQL scripts.
- get_uuid: generates unique execution identifiers.
- get_timestamp: returns standardized timestamps.
"""

import os
import uuid
import datetime
import pyodbc
import configparser


# ==============================================================
# 1. Read SQL script from .sql file
# ==============================================================

def read_sql_file(filepath: str) -> str:
    """
    Load SQL content from a .sql file.

    Parameters
    ----------
    filepath : str
        Path to the SQL file.

    Returns
    -------
    str
        The entire SQL script as a single string.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"SQL file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as file:
        return file.read()


# ==============================================================
# 2. Parse database config file (sql_server_config.cfg)
# ==============================================================

def load_db_config(cfg_path: str = "sql_server_config.cfg") -> dict:
    """
    Parse SQL Server configuration from cfg file.

    Parameters
    ----------
    cfg_path : str
        Path to config file.

    Returns
    -------
    dict
        A dictionary containing DB connection parameters.
    """
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    config = configparser.ConfigParser()
    config.read(cfg_path)

    db_cfg = config["sql_server"]

    return {
        "server": db_cfg.get("server"),
        "database": db_cfg.get("database"),
        "username": db_cfg.get("username", ""),
        "password": db_cfg.get("password", ""),
        "driver": db_cfg.get("driver", "ODBC Driver 17 for SQL Server"),
        "trusted_connection": db_cfg.get("trusted_connection", "no"),
        "encrypt": db_cfg.get("encrypt", "optional"),
        "trust_server_certificate": db_cfg.get("trust_server_certificate", "yes")
    }


# ==============================================================
# 3. Create SQL Server connection
# ==============================================================

def create_db_connection(cfg: dict):
    """
    Establish a live SQL Server connection.

    Parameters
    ----------
    cfg : dict
        Result of load_db_config().

    Returns
    -------
    pyodbc.Connection
        Active database connection object.
    """

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
            f"SERVER={cfg['server']}}};"
            f"DATABASE={cfg['database']};"
            f"UID={cfg['username']};"
            f"PWD={cfg['password']};"
            f"Encrypt={cfg['encrypt']};"
            f"TrustServerCertificate={cfg['trust_server_certificate']};"
        )

    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        raise ConnectionError(f"Failed to connect to SQL Server: {e}")


# ==============================================================
# 4. Execute SQL script with parameters
# ==============================================================

def execute_sql_with_params(conn, sql_text: str, params: dict = None):
    """
    Execute SQL with parameter substitution.

    Parameters
    ----------
    conn : pyodbc.Connection
        Open database connection.

    sql_text : str
        The SQL script content.

    params : dict
        Dictionary of parameters to replace inside SQL script.

    Returns
    -------
    None
    """
    cursor = conn.cursor()

    if params:
        for key, value in params.items():
            placeholder = f"@{key}"
            sql_text = sql_text.replace(placeholder, f"'{value}'")

    try:
        cursor.execute(sql_text)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed executing SQL script: {e}")


# ==============================================================
# 5. Generate unique execution UUID
# ==============================================================

def get_uuid() -> str:
    """Return a unique execution UUID string."""
    return str(uuid.uuid4())


# ==============================================================
# 6. Standardized timestamp helper
# ==============================================================

def get_timestamp() -> str:
    """Return current timestamp in ISO format."""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
