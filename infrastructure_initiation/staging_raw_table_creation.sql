/* ===========================================================
   STAGING RAW TABLES CREATION SCRIPT
   DS206 Project 2 — Group 1
   =========================================================== */

DROP TABLE IF EXISTS staging_Categories;
DROP TABLE IF EXISTS staging_Customers;
DROP TABLE IF EXISTS staging_Employees;
DROP TABLE IF EXISTS staging_OrderDetails;
DROP TABLE IF EXISTS staging_Orders;
DROP TABLE IF EXISTS staging_Products;
DROP TABLE IF EXISTS staging_Region;
DROP TABLE IF EXISTS staging_Shippers;
DROP TABLE IF EXISTS staging_Suppliers;
DROP TABLE IF EXISTS staging_Territories;

---------------------------------------------------------------
-- 1. Categories
---------------------------------------------------------------
CREATE TABLE staging_Categories (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX)
);

---------------------------------------------------------------
-- 2. Customers
---------------------------------------------------------------
CREATE TABLE staging_Customers (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(50),
    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50)
);

---------------------------------------------------------------
-- 3. Employees
---------------------------------------------------------------
CREATE TABLE staging_Employees (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    LastName NVARCHAR(255),
    FirstName NVARCHAR(255),
    Title NVARCHAR(255),
    TitleOfCourtesy NVARCHAR(255),
    BirthDate DATETIME,
    HireDate DATETIME,
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    HomePhone NVARCHAR(50),
    Extension NVARCHAR(20),
    Notes NVARCHAR(MAX),
    ReportsTo INT,
    PhotoPath NVARCHAR(255)
);

---------------------------------------------------------------
-- 4. Order Details
---------------------------------------------------------------
CREATE TABLE staging_OrderDetails (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    UnitPrice DECIMAL(10,2),
    Quantity INT,
    Discount FLOAT
);

---------------------------------------------------------------
-- 5. Orders
---------------------------------------------------------------
CREATE TABLE staging_Orders (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    CustomerID NVARCHAR(50),
    EmployeeID INT,
    OrderDate DATETIME,
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    ShipVia INT,
    Freight DECIMAL(10,2),
    ShipName NVARCHAR(255),
    ShipAddress NVARCHAR(255),
    ShipCity NVARCHAR(255),
    ShipRegion NVARCHAR(255),
    ShipPostalCode NVARCHAR(20),
    ShipCountry NVARCHAR(255),
    TerritoryID NVARCHAR(50)
);

---------------------------------------------------------------
-- 6. Products
---------------------------------------------------------------
CREATE TABLE staging_Products (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName NVARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice DECIMAL(10,2),
    UnitsInStock INT,
    UnitsOnOrder INT,
    ReorderLevel INT,
    Discontinued BIT
);

---------------------------------------------------------------
-- 7. Region
---------------------------------------------------------------
CREATE TABLE staging_Region (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT,
    RegionDescription NVARCHAR(255),
    RegionCategory NVARCHAR(255),
    RegionImportance NVARCHAR(255)
);

---------------------------------------------------------------
-- 8. Shippers
---------------------------------------------------------------
CREATE TABLE staging_Shippers (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT,
    CompanyName NVARCHAR(255),
    Phone NVARCHAR(50)
);

---------------------------------------------------------------
-- 9. Suppliers
---------------------------------------------------------------
CREATE TABLE staging_Suppliers (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT,
    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),
    HomePage NVARCHAR(MAX)
);

---------------------------------------------------------------
-- 10. Territories
---------------------------------------------------------------
CREATE TABLE staging_Territories (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID NVARCHAR(50),
    TerritoryDescription NVARCHAR(255),
    TerritoryCode NVARCHAR(255),
    RegionID INT
);
