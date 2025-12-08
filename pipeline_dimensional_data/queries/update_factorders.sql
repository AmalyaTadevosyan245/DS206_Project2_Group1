/* ===========================================================
   update_factorders.sql
   SNAPSHOT FACT — FactOrders
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS
-- @database_name
-- @schema_name
-- @fact_table_name
-- @orders_staging_table
-- @details_staging_table
---------------------------------------------------------------


---------------------------------------------------------------
-- 1. Ensure SOR entry exists for FactOrders snapshot
---------------------------------------------------------------
INSERT INTO @database_name.@schema_name.Dim_SOR (SOR_Name)
SELECT 'FACT_ORDERS_SNAPSHOT'
WHERE NOT EXISTS (
    SELECT 1
    FROM @database_name.@schema_name.Dim_SOR
    WHERE SOR_Name = 'FACT_ORDERS_SNAPSHOT'
);


---------------------------------------------------------------
-- 2. Retrieve SOR_SK
---------------------------------------------------------------
DECLARE @SOR_SK INT;

SELECT @SOR_SK = SOR_SK
FROM @database_name.@schema_name.Dim_SOR
WHERE SOR_Name = 'FACT_ORDERS_SNAPSHOT';


---------------------------------------------------------------
-- 3. TRUNCATE snapshot fact table (full refresh)
---------------------------------------------------------------
TRUNCATE TABLE @database_name.@schema_name.@fact_table_name;


---------------------------------------------------------------
-- 4. Insert new snapshot rows (no date filtering)
---------------------------------------------------------------
INSERT INTO @database_name.@schema_name.@fact_table_name (
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

    @SOR_SK AS SOR_SK,
    d.staging_raw_id_sk,
    GETDATE() AS LoadDate

FROM @database_name.@schema_name.@details_staging_table d
JOIN @database_name.@schema_name.@orders_staging_table o
      ON o.OrderID = d.OrderID

LEFT JOIN @database_name.@schema_name.DimCustomers dc
       ON dc.Customer_NK = o.CustomerID
      AND dc.IsCurrent = 1

LEFT JOIN @database_name.@schema_name.DimEmployees de
       ON de.Employee_NK = o.EmployeeID
      AND de.IsDeleted = 0

LEFT JOIN @database_name.@schema_name.DimProducts dp
       ON dp.Product_NK = d.ProductID
      AND dp.IsCurrent = 1

LEFT JOIN @database_name.@schema_name.DimShippers ds
       ON ds.Shipper_NK = o.ShipVia

LEFT JOIN @database_name.@schema_name.DimTerritories dt
       ON dt.Territory_NK = o.TerritoryID

LEFT JOIN @database_name.@schema_name.DimRegion dr
       ON dr.Region_NK = o.ShipRegion;