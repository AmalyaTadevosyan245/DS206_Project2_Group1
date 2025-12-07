/* ===========================================================
   update_dim_employees.sql  
   SCD1 with Delete Flag — DimEmployees
   =========================================================== */

---------------------------------------------------------------
-- PARAMETERS passed from Python:
-- @database_name
-- @schema_name
-- @dim_table_name      (DimEmployees)
-- @staging_table_name  (staging_Employees)
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
-- 3. MERGE (SCD1 + Delete)
---------------------------------------------------------------

SET @SQL = '
MERGE ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' AS TARGET
USING (
    SELECT
        EmployeeID AS Employee_NK,
        LastName,
        FirstName,
        Title,
        TitleOfCourtesy,
        BirthDate,
        HireDate,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        HomePhone,
        Extension,
        Notes,
        ReportsTo,
        staging_raw_id_sk
    FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + '
) AS SOURCE
ON TARGET.Employee_NK = SOURCE.Employee_NK

WHEN MATCHED AND (
       ISNULL(TARGET.LastName,'''')        <> ISNULL(SOURCE.LastName,'''')
    OR ISNULL(TARGET.FirstName,'''')       <> ISNULL(SOURCE.FirstName,'''')
    OR ISNULL(TARGET.Title,'''')           <> ISNULL(SOURCE.Title,'''')
    OR ISNULL(TARGET.TitleOfCourtesy,'''') <> ISNULL(SOURCE.TitleOfCourtesy,'''')
    OR ISNULL(TARGET.BirthDate,'''')       <> ISNULL(SOURCE.BirthDate,'''')
    OR ISNULL(TARGET.HireDate,'''')        <> ISNULL(SOURCE.HireDate,'''')
    OR ISNULL(TARGET.Address,'''')         <> ISNULL(SOURCE.Address,'''')
    OR ISNULL(TARGET.City,'''')            <> ISNULL(SOURCE.City,'''')
    OR ISNULL(TARGET.Region,'''')          <> ISNULL(SOURCE.Region,'''')
    OR ISNULL(TARGET.PostalCode,'''')      <> ISNULL(SOURCE.PostalCode,'''')
    OR ISNULL(TARGET.Country,'''')         <> ISNULL(SOURCE.Country,'''')
    OR ISNULL(TARGET.HomePhone,'''')       <> ISNULL(SOURCE.HomePhone,'''')
    OR ISNULL(TARGET.Extension,'''')       <> ISNULL(SOURCE.Extension,'''')
    OR ISNULL(TARGET.Notes,'''')           <> ISNULL(SOURCE.Notes,'''')
    OR ISNULL(TARGET.ReportsTo,-1)         <> ISNULL(SOURCE.ReportsTo,-1)
    OR TARGET.IsDeleted = 1
)
THEN UPDATE SET
       TARGET.LastName        = SOURCE.LastName,
       TARGET.FirstName       = SOURCE.FirstName,
       TARGET.Title           = SOURCE.Title,
       TARGET.TitleOfCourtesy = SOURCE.TitleOfCourtesy,
       TARGET.BirthDate       = SOURCE.BirthDate,
       TARGET.HireDate        = SOURCE.HireDate,
       TARGET.Address         = SOURCE.Address,
       TARGET.City            = SOURCE.City,
       TARGET.Region          = SOURCE.Region,
       TARGET.PostalCode      = SOURCE.PostalCode,
       TARGET.Country         = SOURCE.Country,
       TARGET.HomePhone       = SOURCE.HomePhone,
       TARGET.Extension       = SOURCE.Extension,
       TARGET.Notes           = SOURCE.Notes,
       TARGET.ReportsTo       = SOURCE.ReportsTo,
       TARGET.IsDeleted       = 0,
       TARGET.SOR_SK          = ' + CAST(@SOR_SK AS NVARCHAR) + ',
       TARGET.staging_raw_id_sk = SOURCE.staging_raw_id_sk,
       TARGET.LoadDate        = GETDATE()

WHEN NOT MATCHED BY TARGET
THEN INSERT (
        Employee_NK,
        LastName,
        FirstName,
        Title,
        TitleOfCourtesy,
        BirthDate,
        HireDate,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        HomePhone,
        Extension,
        Notes,
        ReportsTo,
        IsDeleted,
        SOR_SK,
        staging_raw_id_sk,
        LoadDate
    )
    VALUES (
        SOURCE.Employee_NK,
        SOURCE.LastName,
        SOURCE.FirstName,
        SOURCE.Title,
        SOURCE.TitleOfCourtesy,
        SOURCE.BirthDate,
        SOURCE.HireDate,
        SOURCE.Address,
        SOURCE.City,
        SOURCE.Region,
        SOURCE.PostalCode,
        SOURCE.Country,
        SOURCE.HomePhone,
        SOURCE.Extension,
        SOURCE.Notes,
        SOURCE.ReportsTo,
        0,
        ' + CAST(@SOR_SK AS NVARCHAR) + ',
        SOURCE.staging_raw_id_sk,
        GETDATE()
    );

---------------------------------------------------------------
-- 4. Soft-delete employees missing in staging
---------------------------------------------------------------
UPDATE T
SET T.IsDeleted = 1,
    T.SOR_SK     = ' + CAST(@SOR_SK AS NVARCHAR) + ',
    T.LoadDate   = GETDATE()
FROM ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@dim_table_name) + ' T
LEFT JOIN ' + QUOTENAME(@database_name) + '.' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@staging_table_name) + ' S
    ON S.EmployeeID = T.Employee_NK
WHERE S.EmployeeID IS NULL
  AND T.IsDeleted = 0;
';

EXEC(@SQL);