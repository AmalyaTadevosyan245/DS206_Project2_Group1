/* ===========================================================
   update_dim_shippers.sql
   SCD1 — DimShippers (Group 1)
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @dim_table_name        (DimShippers)
-- @staging_table_name    (staging_Shippers)
---------------------------------------------------------------

DECLARE @SQL NVARCHAR(MAX);
DECLARE @SOR_SK INT;

---------------------------------------------------------------
-- 1. Ensure SOR entry exists for this staging table
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
-- 2. Retrieve SOR_SK
---------------------------------------------------------------
SELECT @SOR_SK = SOR_SK
FROM   ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
WHERE  SOR_Name = @staging_table_name;

---------------------------------------------------------------
-- 3. MERGE (SCD1)
---------------------------------------------------------------

SET @SQL = '
MERGE ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' AS TARGET
USING (
    SELECT
        ShipperID AS Shipper_NK,
        CompanyName,
        Phone,
        staging_raw_id_sk
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
) AS SOURCE
ON TARGET.Shipper_NK = SOURCE.Shipper_NK

WHEN MATCHED AND (
       ISNULL(TARGET.CompanyName,'''') <> ISNULL(SOURCE.CompanyName,'''')
    OR ISNULL(TARGET.Phone,'''')       <> ISNULL(SOURCE.Phone,'''')
)
THEN UPDATE SET
       TARGET.CompanyName      = SOURCE.CompanyName,
       TARGET.Phone            = SOURCE.Phone,
       TARGET.SOR_SK           = ' + CAST(@SOR_SK AS NVARCHAR) + ',
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate         = GETDATE()

WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Shipper_NK,
        CompanyName,
        Phone,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Shipper_NK,
        SOURCE.CompanyName,
        SOURCE.Phone,
        ' + CAST(@SOR_SK AS NVARCHAR) + ',
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );
';

EXEC(@SQL);