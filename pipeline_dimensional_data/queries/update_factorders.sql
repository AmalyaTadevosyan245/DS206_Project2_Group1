/* ===========================================================
   update_factorders.sql
   SNAPSHOT FACT — FactOrders
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @fact_table_name         (FactOrders)
-- @orders_staging_table    (staging_Orders)
-- @details_staging_table   (staging_OrderDetails)
---------------------------------------------------------------

DECLARE @SQL NVARCHAR(MAX);
DECLARE @SOR_SK INT;

---------------------------------------------------------------
-- 1. Ensure SOR entry exists for FactOrders snapshot load
---------------------------------------------------------------
SET @SQL = '
INSERT INTO ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR (SOR_Name)
SELECT ''FACT_ORDERS_SNAPSHOT''
WHERE NOT EXISTS (
    SELECT 1 FROM ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
    WHERE SOR_Name = ''FACT_ORDERS_SNAPSHOT''
);';

EXEC(@SQL);

---------------------------------------------------------------
-- 2. Retrieve SOR_SK for snapshot
---------------------------------------------------------------
SELECT @SOR_SK = SOR_SK
FROM   ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
WHERE  SOR_Name = ''FACT_ORDERS_SNAPSHOT'';

---------------------------------------------------------------
-- 3. TRUNCATE snapshot fact table
---------------------------------------------------------------
SET @SQL = '
TRUNCATE TABLE ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@fact_table_name) + ';
';
EXEC(@SQL);

---------------------------------------------------------------
-- 4. Insert new snapshot rows
---------------------------------------------------------------

SET @SQL = '
INSERT INTO ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@fact_table_name) + ' (
    Order_NK,
    Product_NK,
    Customer_SK,
    Employee_SK,
    Product_SK,
    Shipper_SK,
    Territory_SK,
    Region_SK,
    OrderDate,
    RequiredDate,
    ShippedDate,
    Freight,
    UnitPrice,
    Quantity,
    Discount,
    SOR_SK,
    staging_raw_id_sk,
    LoadDate
)

SELECT
    o.OrderID        AS Order_NK,
    d.ProductID      AS Product_NK,

    dc.Customer_SK,
    de.Employee_SK,
    dp.Product_SK,
    ds.Shipper_SK,
    dt.Territory_SK,
    dr.Region_SK,

    o.OrderDate,
    o.RequiredDate,
    o.ShippedDate,
    o.Freight,

    d.UnitPrice,
    d.Quantity,
    d.Discount,

    ' + CAST(@SOR_SK AS NVARCHAR) + ' AS SOR_SK,
    d.staging_raw_id_sk AS staging_raw_id_sk,
    GETDATE() AS LoadDate

FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@details_staging_table) + ' d
JOIN ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@orders_staging_table) + ' o
    ON o.OrderID = d.OrderID

LEFT JOIN ' + QUOTENAME(@database_name) + '.dbo.DimCustomers dc
    ON dc.Customer_NK = o.CustomerID AND dc.IsCurrent = 1

LEFT JOIN ' + QUOTENAME(@database_name) + '.dbo.DimEmployees de
    ON de.Employee_NK = o.EmployeeID AND de.IsDeleted = 0

LEFT JOIN ' + QUOTENAME(@database_name) + '.dbo.DimProducts dp
    ON dp.Product_NK = d.ProductID AND dp.IsCurrent = 1

LEFT JOIN ' + QUOTENAME(@database_name) + '.dbo.DimShippers ds
    ON ds.Shipper_NK = o.ShipVia

LEFT JOIN ' + QUOTENAME(@database_name) + '.dbo.DimTerritories dt
    ON dt.Territory_NK = o.TerritoryID

LEFT JOIN ' + QUOTENAME(@database_name) + '.dbo.DimRegion dr
    ON dr.Region_NK = o.ShipRegion;
';

EXEC(@SQL);