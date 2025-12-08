/* ===========================================================
   update_dim_suppliers.sql
   SCD4 — DimSuppliers
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
-- 3. MERGE — SCD4 (overwrite existing record)
---------------------------------------------------------------
MERGE @database_name.@schema_name.@dim_table_name AS TARGET
USING (
    SELECT
        SupplierID AS Supplier_NK,
        CompanyName,
        ContactName,
        ContactTitle,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        Phone,
        Fax,
        HomePage,
        staging_raw_id_sk
    FROM @database_name.@schema_name.@staging_table_name
) AS SOURCE
ON TARGET.Supplier_NK = SOURCE.Supplier_NK


---------------------------------------------------------------
-- 3A. Update existing rows when any attribute changed
---------------------------------------------------------------
WHEN MATCHED AND (
       ISNULL(TARGET.CompanyName,'')   <> ISNULL(SOURCE.CompanyName,'')
    OR ISNULL(TARGET.ContactName,'')   <> ISNULL(SOURCE.ContactName,'')
    OR ISNULL(TARGET.ContactTitle,'')  <> ISNULL(SOURCE.ContactTitle,'')
    OR ISNULL(TARGET.Address,'')       <> ISNULL(SOURCE.Address,'')
    OR ISNULL(TARGET.City,'')          <> ISNULL(SOURCE.City,'')
    OR ISNULL(TARGET.Region,'')        <> ISNULL(SOURCE.Region,'')
    OR ISNULL(TARGET.PostalCode,'')    <> ISNULL(SOURCE.PostalCode,'')
    OR ISNULL(TARGET.Country,'')       <> ISNULL(SOURCE.Country,'')
    OR ISNULL(TARGET.Phone,'')         <> ISNULL(SOURCE.Phone,'')
    OR ISNULL(TARGET.Fax,'')           <> ISNULL(SOURCE.Fax,'')
    OR ISNULL(TARGET.HomePage,'')      <> ISNULL(SOURCE.HomePage,'')
)
THEN UPDATE SET
       TARGET.CompanyName       = SOURCE.CompanyName,
       TARGET.ContactName       = SOURCE.ContactName,
       TARGET.ContactTitle      = SOURCE.ContactTitle,
       TARGET.Address           = SOURCE.Address,
       TARGET.City              = SOURCE.City,
       TARGET.Region            = SOURCE.Region,
       TARGET.PostalCode        = SOURCE.PostalCode,
       TARGET.Country           = SOURCE.Country,
       TARGET.Phone             = SOURCE.Phone,
       TARGET.Fax               = SOURCE.Fax,
       TARGET.HomePage          = SOURCE.HomePage,
       TARGET.SOR_SK            = @SOR_SK,
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate          = GETDATE()


---------------------------------------------------------------
-- 3B. Insert new Supplier NK rows
---------------------------------------------------------------
WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Supplier_NK,
        CompanyName,
        ContactName,
        ContactTitle,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        Phone,
        Fax,
        HomePage,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Supplier_NK,
        SOURCE.CompanyName,
        SOURCE.ContactName,
        SOURCE.ContactTitle,
        SOURCE.Address,
        SOURCE.City,
        SOURCE.Region,
        SOURCE.PostalCode,
        SOURCE.Country,
        SOURCE.Phone,
        SOURCE.Fax,
        SOURCE.HomePage,
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );