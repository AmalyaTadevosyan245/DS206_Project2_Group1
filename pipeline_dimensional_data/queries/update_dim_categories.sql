/* ===========================================================
   update_dim_categories.sql  
   SCD1 with Delete Handling — DimCategories
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS (from Python)
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
-- 2. Fetch SOR_SK
---------------------------------------------------------------
DECLARE @SOR_SK INT;

SELECT @SOR_SK = SOR_SK
FROM @database_name.@schema_name.Dim_SOR
WHERE SOR_Name = '@staging_table_name';

---------------------------------------------------------------
-- 3. MERGE — SCD1 with delete-flag reset
---------------------------------------------------------------
MERGE @database_name.@schema_name.@dim_table_name AS TARGET
USING (
    SELECT
        CategoryID AS Category_NK,
        CategoryName,
        Description,
        staging_raw_id_sk
    FROM @database_name.@schema_name.@staging_table_name
) AS SOURCE
ON TARGET.Category_NK = SOURCE.Category_NK

-- Update changed or previously deleted rows
WHEN MATCHED AND (
       ISNULL(TARGET.CategoryName, '') <> ISNULL(SOURCE.CategoryName, '')
    OR ISNULL(TARGET.Description, '')  <> ISNULL(SOURCE.Description, '')
    OR TARGET.IsDeleted = 1
)
THEN UPDATE SET
       TARGET.CategoryName      = SOURCE.CategoryName,
       TARGET.Description       = SOURCE.Description,
       TARGET.IsDeleted         = 0,
       TARGET.SOR_SK            = @SOR_SK,
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate          = GETDATE()

-- Insert new rows
WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Category_NK,
        CategoryName,
        Description,
        IsDeleted,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
     )
     VALUES (
        SOURCE.Category_NK,
        SOURCE.CategoryName,
        SOURCE.Description,
        0,
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
     );

---------------------------------------------------------------
-- 4. Soft-delete rows missing in staging
---------------------------------------------------------------
UPDATE T
SET    T.IsDeleted = 1,
       T.LoadDate  = GETDATE(),
       T.SOR_SK    = @SOR_SK
FROM   @database_name.@schema_name.@dim_table_name T
LEFT JOIN @database_name.@schema_name.@staging_table_name S
       ON S.CategoryID = T.Category_NK
WHERE  S.CategoryID IS NULL
  AND  T.IsDeleted = 0;