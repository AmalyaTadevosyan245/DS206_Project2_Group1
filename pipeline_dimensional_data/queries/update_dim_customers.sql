/* ===========================================================
   update_dim_customers.sql
   SCD2 (Historical) — DimCustomers (Group 1)
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @dim_table_name        (DimCustomers)
-- @staging_table_name    (staging_Customers)
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
-- 2. Retrieve SOR_SK
---------------------------------------------------------------
SELECT @SOR_SK = SOR_SK
FROM   ' + QUOTENAME(@database_name) + '.dbo.Dim_SOR
WHERE  SOR_Name = @staging_table_name;

---------------------------------------------------------------
-- 3. MERGE logic for SCD2 (detect changes)
---------------------------------------------------------------

SET @SQL = '
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
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
),

CURRENT_ROWS AS (
    SELECT *
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + '
    WHERE IsCurrent = 1
)

MERGE CURRENT_ROWS AS TARGET
USING INCOMING AS SOURCE
ON TARGET.Customer_NK = SOURCE.Customer_NK

---------------------------------------------------------------
-- 3A. Change detected → close old row & insert new version
---------------------------------------------------------------
WHEN MATCHED AND (
       ISNULL(TARGET.CompanyName,'''') <> ISNULL(SOURCE.CompanyName,'''')
    OR ISNULL(TARGET.ContactName,'''') <> ISNULL(SOURCE.ContactName,'''')
    OR ISNULL(TARGET.ContactTitle,'''') <> ISNULL(SOURCE.ContactTitle,'''')
    OR ISNULL(TARGET.Address,'''') <> ISNULL(SOURCE.Address,'''')
    OR ISNULL(TARGET.City,'''') <> ISNULL(SOURCE.City,'''')
    OR ISNULL(TARGET.Region,'''') <> ISNULL(SOURCE.Region,'''')
    OR ISNULL(TARGET.PostalCode,'''') <> ISNULL(SOURCE.PostalCode,'''')
    OR ISNULL(TARGET.Country,'''') <> ISNULL(SOURCE.Country,'''')
    OR ISNULL(TARGET.Phone,'''') <> ISNULL(SOURCE.Phone,'''')
    OR ISNULL(TARGET.Fax,'''') <> ISNULL(SOURCE.Fax,'''')
)
THEN 
    UPDATE SET
        TARGET.ValidTo = GETDATE(),
        TARGET.IsCurrent = 0

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
        GETDATE(),         -- ValidFrom
        ''9999-12-31'',     -- ValidTo open-ended
        1,                 -- IsCurrent
        ' + CAST(@SOR_SK AS NVARCHAR) + ',
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );
';

EXEC(@SQL);

---------------------------------------------------------------
-- 4. Insert new versions for customers whose old rows were closed
---------------------------------------------------------------
SET @SQL = '
INSERT INTO ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' (
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
    i.Customer_NK,
    i.CompanyName,
    i.ContactName,
    i.ContactTitle,
    i.Address,
    i.City,
    i.Region,
    i.PostalCode,
    i.Country,
    i.Phone,
    i.Fax,
    GETDATE(),
    ''9999-12-31'',
    1,
    ' + CAST(@SOR_SK AS NVARCHAR) + ',
    i.staging_raw_id_sk,
    GETDATE()
FROM (
    SELECT CustomerID, staging_raw_id_sk,
           CompanyName, ContactName, ContactTitle,
           Address, City, Region, PostalCode,
           Country, Phone, Fax
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
) i
WHERE EXISTS (
    SELECT 1
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + '
    WHERE Customer_NK = i.CustomerID
      AND IsCurrent = 0   -- old version was closed
      AND ValidTo = CONVERT(DATE, GETDATE())
);
';

EXEC(@SQL);