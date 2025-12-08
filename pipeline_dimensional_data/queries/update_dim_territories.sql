/* ===========================================================
   update_dim_territories.sql
   SCD3 — One Historical Attribute (TerritoryDescription)
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS:
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
-- 3. MERGE (Unified SCD3 Logic)
---------------------------------------------------------------
MERGE @database_name.@schema_name.@dim_table_name AS TARGET
USING (
    SELECT
        TerritoryID AS Territory_NK,
        TerritoryDescription,
        TerritoryCode,
        RegionID AS Region_NK,
        staging_raw_id_sk
    FROM @database_name.@schema_name.@staging_table_name
) AS SOURCE
ON TARGET.Territory_NK = SOURCE.Territory_NK


---------------------------------------------------------------
-- SINGLE MATCHED CLAUSE 
---------------------------------------------------------------
WHEN MATCHED AND (
       ISNULL(TARGET.TerritoryDescription_Current,'') <> ISNULL(SOURCE.TerritoryDescription,'')
    OR ISNULL(TARGET.TerritoryCode,'') <> ISNULL(SOURCE.TerritoryCode,'')
    OR ISNULL(TARGET.Region_NK,-1) <> ISNULL(SOURCE.Region_NK,-1)
)
THEN UPDATE SET

       -- Only shift prior value when description actually changed
       TARGET.TerritoryDescription_Prior =
            CASE
                WHEN ISNULL(TARGET.TerritoryDescription_Current,'') 
                        <> ISNULL(SOURCE.TerritoryDescription,'')
                THEN TARGET.TerritoryDescription_Current
                ELSE TARGET.TerritoryDescription_Prior
            END,

       -- Always update current values
       TARGET.TerritoryDescription_Current = SOURCE.TerritoryDescription,
       TARGET.TerritoryCode                = SOURCE.TerritoryCode,
       TARGET.Region_NK                    = SOURCE.Region_NK,

       TARGET.SOR_SK                       = @SOR_SK,
       TARGET.staging_raw_id_sk            = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate                     = GETDATE()


---------------------------------------------------------------
-- INSERT NEW ROWS
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
        SOURCE.TerritoryDescription,
        NULL,
        SOURCE.TerritoryCode,
        SOURCE.Region_NK,
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );