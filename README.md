# DS206 Dimensional Modeling and ETL Pipeline

This project implements a complete end-to-end ETL and dimensional modeling pipeline using SQL Server and Python. 

## 1. Staging Layer (Raw Data)
All raw Excel sheets are ingested into SQL Server staging tables. Each staging table contains:
- `staging_raw_id_sk` (INT IDENTITY PK)
- Natural keys and attributes exactly as provided in the Excel file

Staging tables:
`staging_Categories`, `staging_Customers`, `staging_Employees`, `staging_OrderDetails`, `staging_Orders`, `staging_Products`, `staging_Region`, `staging_Shippers`, `staging_Suppliers`, `staging_Territories`.

## 2. Dimensional Database Schema
The dimensional model resides in the `ORDER_DDS` database. All dimension tables include:
- A surrogate key (e.g., Category_SK)
- A natural key (e.g., Category_NK)
- `staging_raw_id_sk`
- `SOR_SK` foreign key to `Dim_SOR`
- Required SCD fields depending on dimension type

Dimension SCD configurations:
- **DimCategories**: SCD1 with soft delete
- **DimCustomers**: SCD2 with historical tracking
- **DimEmployees**: SCD1 with soft delete
- **DimProducts**: SCD2 with record closing
- **DimRegion**: SCD1
- **DimShippers**: SCD1
- **DimSuppliers**: SCD4 mini-dimension
- **DimTerritories**: SCD3 (current and prior attribute)

The fact table (`FactOrders`) is modeled as a snapshot fact table and includes natural keys, dimension surrogate keys, measures, `SOR_SK`, and `staging_raw_id_sk`.

## 3. Dim_SOR Table
The `Dim_SOR` table stores Source-Of-Record names and generates surrogate SOR_SK identifiers.
Every dimension and fact load script inserts or retrieves a `SOR_SK` corresponding to its staging source.

## 4. Parametrized SQL Scripts (pipeline_dimensional_data/queries/)
For every dimension, a parametrized SQL script `update_dim_{table}.sql` is created. Each script:
- Ensures the SOR entry exists
- Performs SCD-appropriate MERGE logic with null-safe comparisons
- Handles inserts, updates, deletes, closings, or history splits
- Includes both `SOR_SK` and `staging_raw_id_sk`

Scripts included:
```
update_dim_categories.sql
update_dim_customers.sql
update_dim_employees.sql
update_dim_products.sql
update_dim_region.sql
update_dim_shippers.sql
update_dim_suppliers.sql
update_dim_territories.sql
update_fact.sql
update_fact_error.sql
```

`update_fact.sql` performs a date-filtered ingestion from staging to the fact table, joining dimensions to obtain surrogate keys. Rows missing valid natural keys are excluded.

`update_fact_error.sql` captures all rejected rows with missing or invalid natural keys.

Both fact loader scripts accept parameters: `database name`, `schema name`, `table name`, `start_date`, and `end_date`.

## 5. Python Component (Next Phase)
Once SQL logic is validated, the following Python modules will orchestrate the pipeline:

- `utils.py`: SQL loading, DB config parsing, UUID generation, connection helpers
- `tasks.py`: Functions for running each parametrized SQL script
- `flow.py`: Defines `DimensionalDataFlow` with sequential execution and `exec(start_date, end_date)`
- `logging.py`: Writes logs to `logs/logs_dimensional_data_pipeline.txt` including execution_id
- `main.py`: CLI interface allowing:
  ```
  python main.py --start_date=YYYY-MM-DD --end_date=YYYY-MM-DD
  ```

Python tasks execute SQL scripts, pass parameters, maintain atomicity, and enforce sequential dependency rules.

## 6. Repository Structure
```
DS206_PROJECT2_GROUP1/
│
├── infrastructure_initiation/
│   ├── dimensional_db_creation.sql
│   ├── dimensional_db_table_creation.sql
│   └── staging_raw_table_creation.sql
│
├── pipeline_dimensional_data/
│   ├── flow.py                # Main ETL flow class
│   ├── tasks.py               # All Python tasks for executing SQL scripts
│   ├── utils.py               # Helper utilities (read SQL, DB connection, UUID)
│   ├── logging.py             # Logger config for ETL pipeline
│   ├── config.py              # Database name, schema name, table mappings
│   │
│   └── queries/               # All parametrized SQL scripts
│       ├── update_dim_categories.sql
│       ├── update_dim_customers.sql
│       ├── update_dim_employees.sql
│       ├── update_dim_products.sql
│       ├── update_dim_region.sql
│       ├── update_dim_shippers.sql
│       ├── update_dim_suppliers.sql
│       ├── update_dim_territories.sql
│       ├── update_fact.sql
│       └── update_fact_error.sql
│
├── logs/
│   └── logs_dimensional_data_pipeline.txt   # Logged runs of ETL flow
│
├── dashboard/
│   └── group1_dashboard.pbix
│
├── main.py                # CLI entry point for the entire pipeline
└── README.md

```

## 7. SQL Component Status
All SQL tables, SCD variations, MERGE logic, and parametrized ingestion scripts are fully implemented according to DS206 requirements. The remaining work consists exclusively of Python orchestration and testing.
