"""
config.py
Central configuration for dimensional ETL pipeline.

Contains:
    - database_name
    - schema_name
    - mapping of dimension tables to staging tables
    - fact table names
"""

database_name = "ORDER_DDS"
schema_name = "dbo"

# Dimension table mapping
DIM_TABLES = {
    "DimCategories": "staging_Categories",
    "DimCustomers": "staging_Customers",
    "DimEmployees": "staging_Employees",
    "DimProducts": "staging_Products",
    "DimRegion": "staging_Region",
    "DimShippers": "staging_Shippers",
    "DimSuppliers": "staging_Suppliers",
    "DimTerritories": "staging_Territories",
}

# Fact tables
FACT_TABLE = "FactOrders"
FACT_ERROR_TABLE = "FactOrders_Error"
STAGING_FACT_TABLE = "staging_Orders"