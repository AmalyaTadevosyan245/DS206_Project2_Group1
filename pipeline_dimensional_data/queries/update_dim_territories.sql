/* ===========================================================
   update_dim_territories.sql
   SCD3 — One Historical Attribute (TerritoryDescription)
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @dim_table_name         (DimTerritories)
-- @staging_table_name     (staging_Territories)
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
-- 3. MERGE (SCD3 logic)
---------------------------------------------------------------

SET @SQL = '
MERGE ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' AS TARGET
USING (
    SELECT
        TerritoryID AS Territory_NK,
        TerritoryDescription,
        TerritoryCode,
        RegionID AS Region_NK,
        staging_raw_id_sk
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
) AS SOURCE
ON TARGET.Territory_NK = SOURCE.Territory_NK

---------------------------------------------------------------
-- 3A. If TerritoryDescription changed → SCD3 behavior
---------------------------------------------------------------
WHEN MATCHED AND 
    ISNULL(TARGET.TerritoryDescription_Current,'''') 
        <> ISNULL(SOURCE.TerritoryDescription,'''')
THEN UPDATE SET
       TARGET.TerritoryDescription_Prior   = TARGET.TerritoryDescription_Current,
       TARGET.TerritoryDescription_Current = SOURCE.TerritoryDescription,
       TARGET.TerritoryCode                = SOURCE.TerritoryCode,
       TARGET.Region_NK                    = SOURCE.Region_NK,
       TARGET.staging_raw_id_sk            = SOURCE.staging_raw_id_sk,
       TARGET.SOR_SK                       = ' + CAST(@SOR_SK AS NVARCHAR) + ',
       TARGET.LoadDate                     = GETDATE()

---------------------------------------------------------------
-- 3B. Other attribute changes (overwrite in place)
---------------------------------------------------------------
WHEN MATCHED AND (
      ISNULL(TARGET.TerritoryCode,'''') <> ISNULL(SOURCE.TerritoryCode,'''')
   OR ISNULL(TARGET.Region_NK,-1)       <> ISNULL(SOURCE.Region_NK,-1)
)
THEN UPDATE SET
       TARGET.TerritoryCode                = SOURCE.TerritoryCode,
       TARGET.Region_NK                    = SOURCE.Region_NK,
       TARGET.staging_raw_id_sk            = SOURCE.staging_raw_id_sk,
       TARGET.SOR_SK                       = ' + CAST(@SOR_SK AS NVARCHAR) + ',
       TARGET.LoadDate                     = GETDATE()

---------------------------------------------------------------
-- 3C. Insert new NK rows
---------------------------------------------------------------
WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Territory_NK,
        TerritoryDescription_Current,
        TerritoryDescription_Prior,
        TerritoryCode,
        Region_NK,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Territory_NK,
        SOURCE.TerritoryDescription,   -- becomes Current
        NULL,                          -- no prior on first load
        SOURCE.TerritoryCode,
        SOURCE.Region_NK,
        ' + CAST(@SOR_SK AS NVARCHAR) + ',
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );
';

EXEC(@SQL);