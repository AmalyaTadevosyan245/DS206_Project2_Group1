/* ===========================================================
   update_dim_region.sql  
   SCD1 — DimRegion (Group 1)
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @dim_table_name        (DimRegion)
-- @staging_table_name    (staging_Region)
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
-- 3. MERGE (SCD1)
---------------------------------------------------------------
MERGE @database_name.@schema_name.@dim_table_name AS TARGET
USING (
    SELECT
        RegionID AS Region_NK,
        RegionDescription,
        RegionCategory,
        RegionImportance,
        staging_raw_id_sk
    FROM @database_name.@schema_name.@staging_table_name
) AS SOURCE
ON TARGET.Region_NK = SOURCE.Region_NK

-- Update changed records
WHEN MATCHED AND (
       ISNULL(TARGET.RegionDescription, '') <> ISNULL(SOURCE.RegionDescription, '')
    OR ISNULL(TARGET.RegionCategory, '')    <> ISNULL(SOURCE.RegionCategory, '')
    OR ISNULL(TARGET.RegionImportance, '')  <> ISNULL(SOURCE.RegionImportance, '')
)
THEN UPDATE SET
       TARGET.RegionDescription = SOURCE.RegionDescription,
       TARGET.RegionCategory    = SOURCE.RegionCategory,
       TARGET.RegionImportance  = SOURCE.RegionImportance,
       TARGET.SOR_SK            = @SOR_SK,
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate          = GETDATE()

-- Insert new records
WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Region_NK,
        RegionDescription,
        RegionCategory,
        RegionImportance,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Region_NK,
        SOURCE.RegionDescription,
        SOURCE.RegionCategory,
        SOURCE.RegionImportance,
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );