/* ===========================================================
   update_fact.sql
   SNAPSHOT FACT LOADER WITH DATE FILTERING
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS
-- @database_name
-- @schema_name
-- @fact_table_name
-- @staging_table_name   
-- @start_date
-- @end_date
---------------------------------------------------------------


---------------------------------------------------------------
-- 1. Ensure SOR entry exists
---------------------------------------------------------------
INSERT INTO @database_name.@schema_name.Dim_SOR (SOR_Name)
SELECT 'FACT_ORDERS_SNAPSHOT'
WHERE NOT EXISTS (
    SELECT 1
    FROM @database_name.@schema_name.Dim_SOR
    WHERE SOR_Name = 'FACT_ORDERS_SNAPSHOT'
);


---------------------------------------------------------------
-- 2. Load SOR_SK
---------------------------------------------------------------
DECLARE @SOR_SK INT;

SELECT @SOR_SK = SOR_SK
FROM @database_name.@schema_name.Dim_SOR
WHERE SOR_Name = 'FACT_ORDERS_SNAPSHOT';


---------------------------------------------------------------
-- 3. Delete existing snapshot rows in date range
---------------------------------------------------------------
DELETE FROM @database_name.@schema_name.@fact_table_name
WHERE OrderDate BETWEEN @start_date AND @end_date;


---------------------------------------------------------------
-- 4. Insert refreshed snapshot rows
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
    o.OrderID         AS Order_NK,
    d.ProductID       AS Product_NK,

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

    @SOR_SK,
    d.staging_raw_id_sk,
    GETDATE()

FROM @database_name.@schema_name.staging_OrderDetails d
JOIN @database_name.@schema_name.@staging_table_name o
      ON o.OrderID = d.OrderID
     AND o.OrderDate BETWEEN @start_date AND @end_date

LEFT JOIN @database_name.@schema_name.DimCustomers dc
       ON dc.Customer_NK = o.CustomerID AND dc.IsCurrent = 1

LEFT JOIN @database_name.@schema_name.DimEmployees de
       ON de.Employee_NK = o.EmployeeID AND de.IsDeleted = 0

LEFT JOIN @database_name.@schema_name.DimProducts dp
       ON dp.Product_NK = d.ProductID AND dp.IsCurrent = 1

LEFT JOIN @database_name.@schema_name.DimShippers ds
       ON ds.Shipper_NK = o.ShipVia

LEFT JOIN @database_name.@schema_name.DimTerritories dt
       ON dt.Territory_NK = o.TerritoryID

LEFT JOIN @database_name.@schema_name.DimRegion dr
       ON dr.Region_NK = o.ShipRegion;