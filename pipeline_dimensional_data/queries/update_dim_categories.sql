/* ===========================================================
   update_dim_categories.sql  
   SCD1 with Delete Handling — DimCategories
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS (passed from Python)
---------------------------------------------------------------
-- @database_name
-- @schema_name
-- @dim_table_name     (dim table: DimCategories)
-- @staging_table_name (staging: staging_Categories)

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
-- 2. Get SOR_SK  (must be dynamic SQL)
---------------------------------------------------------------
SET @SQL = '
SELECT @SOR_SK_OUT = SOR_SK
FROM ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
WHERE SOR_Name = ''' + @staging_table_name + ''';';

EXEC sp_executesql @SQL, N'@SOR_SK_OUT INT OUTPUT', @SOR_SK_OUT=@SOR_SK OUTPUT;


---------------------------------------------------------------
-- 3. MERGE (SCD1 + Delete Handling)
---------------------------------------------------------------

SET @SQL = '
MERGE ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' AS TARGET
USING (
    SELECT
        CategoryID AS Category_NK,
        CategoryName,
        Description,
        staging_raw_id_sk
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
) AS SOURCE
ON TARGET.Category_NK = SOURCE.Category_NK

WHEN MATCHED AND (
       ISNULL(TARGET.CategoryName,'''') <> ISNULL(SOURCE.CategoryName,'''')
    OR ISNULL(TARGET.Description ,'''') <> ISNULL(SOURCE.Description ,'''')
    OR TARGET.IsDeleted = 1
)
THEN UPDATE SET
       TARGET.CategoryName      = SOURCE.CategoryName,
       TARGET.Description       = SOURCE.Description,
       TARGET.IsDeleted         = 0,
       TARGET.SOR_SK            = ' + CAST(@SOR_SK AS NVARCHAR) + ',
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate          = GETDATE()

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
        ' + CAST(@SOR_SK AS NVARCHAR) + ',
        SOURCE.staging_raw_id_sk,
        GETDATE()
     );

---------------------------------------------------------------
-- 4. Soft-delete rows not present in staging  (DYNAMIC SQL)
---------------------------------------------------------------

SET @SQL = '
UPDATE T
SET    T.IsDeleted = 1,
       T.LoadDate  = GETDATE(),
       T.SOR_SK    = ' + CAST(@SOR_SK AS NVARCHAR(MAX)) + '
FROM   ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' T
LEFT JOIN ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + ' S
       ON S.CategoryID = T.Category_NK
WHERE  S.CategoryID IS NULL
  AND  T.IsDeleted = 0;
';

EXEC(@SQL);

