/* ===========================================================
   update_dim_shippers.sql
   SCD1 — DimShippers
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
-- 3. MERGE — SCD1
---------------------------------------------------------------
MERGE @database_name.@schema_name.@dim_table_name AS TARGET
USING (
    SELECT
        ShipperID AS Shipper_NK,
        CompanyName,
        Phone,
        staging_raw_id_sk
    FROM @database_name.@schema_name.@staging_table_name
) AS SOURCE
ON TARGET.Shipper_NK = SOURCE.Shipper_NK

-- Update if any attribute changed
WHEN MATCHED AND (
       ISNULL(TARGET.CompanyName,'') <> ISNULL(SOURCE.CompanyName,'')
    OR ISNULL(TARGET.Phone,'')       <> ISNULL(SOURCE.Phone,'')
)
THEN UPDATE SET
       TARGET.CompanyName       = SOURCE.CompanyName,
       TARGET.Phone             = SOURCE.Phone,
       TARGET.SOR_SK            = @SOR_SK,
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate          = GETDATE()

-- Insert new rows
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
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );