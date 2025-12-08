/* ===========================================================
   update_dim_products.sql
   SCD2 + Delete Closing — DimProducts
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS
-- @database_name
-- @schema_name
-- @dim_table_name
-- @staging_table_name
---------------------------------------------------------------


---------------------------------------------------------------
-- 1. Ensure SOR entry exists
---------------------------------------------------------------
INSERT INTO @database_name.@schema_name.Dim_SOR (SOR_Name)
SELECT '@staging_table_name'
WHERE NOT EXISTS (
    SELECT 1
    FROM @database_name.@schema_name.Dim_SOR
    WHERE SOR_Name = '@staging_table_name'
);


---------------------------------------------------------------
-- 2. Retrieve SOR_SK
---------------------------------------------------------------
DECLARE @SOR_SK INT;

SELECT @SOR_SK = SOR_SK
FROM @database_name.@schema_name.Dim_SOR
WHERE SOR_Name = '@staging_table_name';


---------------------------------------------------------------
-- 3. SCD2 MERGE (detect changes → close old version)
---------------------------------------------------------------
;WITH INCOMING AS (
    SELECT
        ProductID AS Product_NK,
        ProductName,
        SupplierID AS Supplier_NK,
        CategoryID AS Category_NK,
        QuantityPerUnit,
        UnitPrice,
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        staging_raw_id_sk
    FROM @database_name.@schema_name.@staging_table_name
),
CURRENT_ROWS AS (
    SELECT *
    FROM @database_name.@schema_name.@dim_table_name
    WHERE IsCurrent = 1
)

MERGE CURRENT_ROWS AS TARGET
USING INCOMING AS SOURCE
ON TARGET.Product_NK = SOURCE.Product_NK

-- 3A. Change detected → close existing version
WHEN MATCHED AND (
       ISNULL(TARGET.ProductName,'')      <> ISNULL(SOURCE.ProductName,'')
    OR ISNULL(TARGET.Supplier_NK,-1)      <> ISNULL(SOURCE.Supplier_NK,-1)
    OR ISNULL(TARGET.Category_NK,-1)      <> ISNULL(SOURCE.Category_NK,-1)
    OR ISNULL(TARGET.QuantityPerUnit,'')  <> ISNULL(SOURCE.QuantityPerUnit,'')
    OR ISNULL(TARGET.UnitPrice,-1)        <> ISNULL(SOURCE.UnitPrice,-1)
    OR ISNULL(TARGET.UnitsInStock,-1)     <> ISNULL(SOURCE.UnitsInStock,-1)
    OR ISNULL(TARGET.UnitsOnOrder,-1)     <> ISNULL(SOURCE.UnitsOnOrder,-1)
    OR ISNULL(TARGET.ReorderLevel,-1)     <> ISNULL(SOURCE.ReorderLevel,-1)
    OR ISNULL(TARGET.Discontinued,'')     <> ISNULL(SOURCE.Discontinued,'')
)
THEN UPDATE SET
       TARGET.ValidTo   = GETDATE(),
       TARGET.IsCurrent = 0

-- 3B. Insert new NKs or fresh versions
WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Product_NK,
        ProductName,
        Supplier_NK,
        Category_NK,
        QuantityPerUnit,
        UnitPrice,
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        ValidFrom,
        ValidTo,
        IsCurrent,
        IsClosed,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Product_NK,
        SOURCE.ProductName,
        SOURCE.Supplier_NK,
        SOURCE.Category_NK,
        SOURCE.QuantityPerUnit,
        SOURCE.UnitPrice,
        SOURCE.UnitsInStock,
        SOURCE.UnitsOnOrder,
        SOURCE.ReorderLevel,
        SOURCE.Discontinued,
        GETDATE(),           -- ValidFrom
        '9999-12-31',        -- ValidTo open-ended
        1,                   -- IsCurrent
        0,                   -- IsClosed
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );


---------------------------------------------------------------
-- 4. Insert new versions for rows closed today (new SCD2 versions)
---------------------------------------------------------------
INSERT INTO @database_name.@schema_name.@dim_table_name (
        Product_NK,
        ProductName,
        Supplier_NK,
        Category_NK,
        QuantityPerUnit,
        UnitPrice,
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        ValidFrom,
        ValidTo,
        IsCurrent,
        IsClosed,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
)
SELECT 
    S.ProductID,
    S.ProductName,
    S.SupplierID,
    S.CategoryID,
    S.QuantityPerUnit,
    S.UnitPrice,
    S.UnitsInStock,
    S.UnitsOnOrder,
    S.ReorderLevel,
    S.Discontinued,
    GETDATE(),
    '9999-12-31',
    1,              -- new current row
    0,              -- not closed
    @SOR_SK,
    S.staging_raw_id_sk,
    GETDATE()
FROM @database_name.@schema_name.@staging_table_name S
WHERE EXISTS (
    SELECT 1
    FROM @database_name.@schema_name.@dim_table_name D
    WHERE D.Product_NK = S.ProductID
      AND D.IsCurrent = 0
      AND CAST(D.ValidTo AS DATE) = CAST(GETDATE() AS DATE)
);


---------------------------------------------------------------
-- 5. Delete Closing — Close products missing from staging
---------------------------------------------------------------
UPDATE T
SET
    T.ValidTo   = GETDATE(),
    T.IsCurrent = 0,
    T.IsClosed  = 1,
    T.SOR_SK    = @SOR_SK,
    T.LoadDate  = GETDATE()
FROM @database_name.@schema_name.@dim_table_name T
LEFT JOIN INCOMING S
       ON S.Product_NK = T.Product_NK
WHERE S.Product_NK IS NULL
  AND T.IsCurrent = 1;