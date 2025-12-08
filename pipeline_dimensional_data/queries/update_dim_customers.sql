/* ===========================================================
   update_dim_customers.sql
   SCD2 (Historical) — DimCustomers
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
-- 2. Fetch SOR_SK
---------------------------------------------------------------
DECLARE @SOR_SK INT;

SELECT @SOR_SK = SOR_SK
FROM @database_name.@schema_name.Dim_SOR
WHERE SOR_Name = '@staging_table_name';

---------------------------------------------------------------
-- 3. SCD2 MERGE Logic — Close old row on change
---------------------------------------------------------------
;WITH INCOMING AS (
    SELECT
        CustomerID AS Customer_NK,
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
ON TARGET.Customer_NK = SOURCE.Customer_NK

-- 3A. Change detected → close the current row
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
)
THEN UPDATE SET
       TARGET.ValidTo      = GETDATE(),
       TARGET.IsCurrent    = 0

-- 3B. New customer → insert fresh version
WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Customer_NK,
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
        ValidFrom,
        ValidTo,
        IsCurrent,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Customer_NK,
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
        GETDATE(),          -- ValidFrom
        '9999-12-31',       -- ValidTo open-ended
        1,                  -- IsCurrent
        @SOR_SK,
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );

---------------------------------------------------------------
-- 4. Insert new versions for closed rows (historical SCD2)
---------------------------------------------------------------
INSERT INTO @database_name.@schema_name.@dim_table_name (
        Customer_NK,
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
        ValidFrom,
        ValidTo,
        IsCurrent,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
)
SELECT 
    S.CustomerID,
    S.CompanyName,
    S.ContactName,
    S.ContactTitle,
    S.Address,
    S.City,
    S.Region,
    S.PostalCode,
    S.Country,
    S.Phone,
    S.Fax,
    GETDATE(),          -- ValidFrom
    '9999-12-31',       -- ValidTo
    1,                  -- IsCurrent
    @SOR_SK,
    S.staging_raw_id_sk,
    GETDATE()
FROM @database_name.@schema_name.@staging_table_name S
WHERE EXISTS (
    SELECT 1
    FROM @database_name.@schema_name.@dim_table_name D
    WHERE D.Customer_NK = S.CustomerID
      AND D.IsCurrent = 0
      AND CAST(D.ValidTo AS DATE) = CAST(GETDATE() AS DATE)
);