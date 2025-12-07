/* ===========================================================
   update_dim_products.sql
   SCD2 + Delete Closing — DimProducts (Group 1)
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @dim_table_name        (DimProducts)
-- @staging_table_name    (staging_Products)
---------------------------------------------------------------

DECLARE @SQL NVARCHAR(MAX);
DECLARE @SOR_SK INT;

---------------------------------------------------------------
-- 1. Ensure SOR entry exists
---------------------------------------------------------------
SET @SQL = '
INSERT INTO ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR (SOR_Name)
SELECT ''' + @staging_table_name + '''
WHERE NOT EXISTS (
    SELECT 1 
    FROM ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
    WHERE SOR_Name = ''' + @staging_table_name + '''
);';

EXEC(@SQL);

---------------------------------------------------------------
-- 2. Get SOR_SK
---------------------------------------------------------------
SELECT @SOR_SK = SOR_SK
FROM   ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
WHERE  SOR_Name = @staging_table_name;

---------------------------------------------------------------
-- 3. MERGE: Detect changes for SCD2
---------------------------------------------------------------

SET @SQL = '
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
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
),

CURRENT_ROWS AS (
    SELECT *
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + '
    WHERE IsCurrent = 1
)

MERGE CURRENT_ROWS AS TARGET
USING INCOMING AS SOURCE
ON TARGET.Product_NK = SOURCE.Product_NK

---------------------------------------------------------------
-- 3A. When a change is detected → close old row
---------------------------------------------------------------
WHEN MATCHED AND (
       ISNULL(TARGET.ProductName,'''')        <> ISNULL(SOURCE.ProductName,'''')
    OR ISNULL(TARGET.Supplier_NK,-1)          <> ISNULL(SOURCE.Supplier_NK,-1)
    OR ISNULL(TARGET.Category_NK,-1)          <> ISNULL(SOURCE.Category_NK,-1)
    OR ISNULL(TARGET.QuantityPerUnit,'''')    <> ISNULL(SOURCE.QuantityPerUnit,'''')
    OR ISNULL(TARGET.UnitPrice,-1)            <> ISNULL(SOURCE.UnitPrice,-1)
    OR ISNULL(TARGET.UnitsInStock,-1)         <> ISNULL(SOURCE.UnitsInStock,-1)
    OR ISNULL(TARGET.UnitsOnOrder,-1)         <> ISNULL(SOURCE.UnitsOnOrder,-1)
    OR ISNULL(TARGET.ReorderLevel,-1)         <> ISNULL(SOURCE.ReorderLevel,-1)
    OR ISNULL(TARGET.Discontinued,'''')       <> ISNULL(SOURCE.Discontinued,'''')
)
THEN UPDATE SET
       TARGET.ValidTo = GETDATE(),
       TARGET.IsCurrent = 0

---------------------------------------------------------------
-- 3B. Insert new rows (new NKs or new versions)
---------------------------------------------------------------
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
        ''9999-12-31'',       -- ValidTo open-ended
        1,                    -- IsCurrent
        0,                    -- IsClosed always 0 on fresh load
        ' + CAST(@SOR_SK AS NVARCHAR) + ',
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );
';

EXEC(@SQL);


---------------------------------------------------------------
-- 4. Insert new versions for products whose old rows were closed
---------------------------------------------------------------
SET @SQL = '
INSERT INTO ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' (
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
    i.Product_NK,
    i.ProductName,
    i.Supplier_NK,
    i.Category_NK,
    i.QuantityPerUnit,
    i.UnitPrice,
    i.UnitsInStock,
    i.UnitsOnOrder,
    i.ReorderLevel,
    i.Discontinued,
    GETDATE(),             -- new version ValidFrom
    ''9999-12-31'',         -- open-ended ValidTo
    1,                      -- IsCurrent
    0,                      -- IsClosed
    ' + CAST(@SOR_SK AS NVARCHAR) + ',
    i.staging_raw_id_sk,
    GETDATE()
FROM INCOMING i
WHERE EXISTS (
    SELECT 1
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + '
    WHERE Product_NK = i.Product_NK
      AND IsCurrent = 0
      AND ValidTo = CONVERT(DATE, GETDATE())   -- closed today
);
';

EXEC(@SQL);


---------------------------------------------------------------
-- 5. Closing logic for missing NKs (DELETE CLOSING requirement)
---------------------------------------------------------------
SET @SQL = '
UPDATE T
SET    T.ValidTo = GETDATE(),
       T.IsCurrent = 0,
       T.IsClosed = 1,
       T.LoadDate = GETDATE(),
       T.SOR_SK = ' + CAST(@SOR_SK AS NVARCHAR) + '
FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' T
LEFT JOIN INCOMING S
       ON S.Product_NK = T.Product_NK
WHERE S.Product_NK IS NULL
  AND T.IsCurrent = 1;
';

EXEC(@SQL);