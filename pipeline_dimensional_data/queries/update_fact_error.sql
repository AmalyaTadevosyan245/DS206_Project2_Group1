USE {{database_name}}

---------------------------------------------------------------
-- 1. Ensure SOR entry exists
---------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1
    FROM {{schema_name}}.Dim_SOR
    WHERE SOR_Name = 'FACT_ORDERS_ERROR'
)
BEGIN
    INSERT INTO {{schema_name}}.Dim_SOR (SOR_Name)
    VALUES ('FACT_ORDERS_ERROR');
END;


---------------------------------------------------------------
-- 2. Load SOR_SK
---------------------------------------------------------------
DECLARE @SOR_SK INT;

SELECT @SOR_SK = SOR_SK
FROM {{schema_name}}.Dim_SOR
WHERE SOR_Name = 'FACT_ORDERS_ERROR';


---------------------------------------------------------------
-- 3. Insert missing-dimension errors
---------------------------------------------------------------

INSERT INTO {{fact_error_table}} (
    Order_NK,
    Product_NK,
    Customer_SK,
    Employee_SK,
    Product_SK,
    Shipper_SK,
    Territory_SK,
    Region_SK,
    ErrorMessage,
    SOR_SK,
    staging_raw_id_sk,
    LoadDate
)
SELECT
    o.OrderID,
    d.ProductID,

    dc.Customer_SK,
    de.Employee_SK,
    dp.Product_SK,
    ds.Shipper_SK,
    dt.Territory_SK,
    dr.Region_SK,

    CASE 
         WHEN dc.Customer_SK IS NULL THEN 'Missing Customer_SK'
         WHEN de.Employee_SK IS NULL THEN 'Missing Employee_SK'
         WHEN dp.Product_SK IS NULL THEN 'Missing Product_SK'
         WHEN ds.Shipper_SK IS NULL THEN 'Missing Shipper_SK'
         WHEN dt.Territory_SK IS NULL THEN 'Missing Territory_SK'
         WHEN dr.Region_SK IS NULL THEN 'Missing Region_SK'
    END,

    @SOR_SK,
    d.staging_raw_id_sk,
    GETDATE()

FROM {{schema_name}}.staging_OrderDetails d
JOIN {{staging_table_name}} o
    ON o.OrderID = d.OrderID
    AND o.OrderDate BETWEEN '{{start_date}}' AND '{{end_date}}'

LEFT JOIN {{schema_name}}.DimCustomers dc
    ON dc.Customer_NK = o.CustomerID AND dc.IsCurrent = 1

LEFT JOIN {{schema_name}}.DimEmployees de
    ON de.Employee_NK = o.EmployeeID AND de.IsDeleted = 0

LEFT JOIN {{schema_name}}.DimProducts dp
    ON dp.Product_NK = d.ProductID AND dp.IsCurrent = 1

LEFT JOIN {{schema_name}}.DimShippers ds
    ON ds.Shipper_NK = o.ShipVia

LEFT JOIN {{schema_name}}.DimTerritories dt
    ON dt.Territory_NK = o.TerritoryID

LEFT JOIN {{schema_name}}.DimRegion dr
    ON dr.Region_NK = o.ShipRegion

WHERE 
      dc.Customer_SK IS NULL
   OR de.Employee_SK IS NULL
   OR dp.Product_SK IS NULL
   OR ds.Shipper_SK IS NULL
   OR dt.Territory_SK IS NULL
   OR dr.Region_SK IS NULL;
