/* ===========================================================
   DIMENSIONAL DATABASE TABLE CREATION 
   =========================================================== */

USE ORDER_DDS;
GO

/* ===========================================================
   CLEANUP
   =========================================================== */
DROP TABLE IF EXISTS FactOrders;
DROP TABLE IF EXISTS DimTerritories;
DROP TABLE IF EXISTS DimShippers;
DROP TABLE IF EXISTS DimRegion;
DROP TABLE IF EXISTS DimProducts;
DROP TABLE IF EXISTS DimSuppliers;
DROP TABLE IF EXISTS DimEmployees;
DROP TABLE IF EXISTS DimCustomers;
DROP TABLE IF EXISTS DimCategories;
DROP TABLE IF EXISTS Dim_SOR;


/* ===========================================================
   DIM SOR — REQUIRED BY ASSIGNMENT
   =========================================================== */

CREATE TABLE Dim_SOR (
    SOR_SK INT IDENTITY(1,1) PRIMARY KEY,
    SOR_Name NVARCHAR(255) NOT NULL
);


/* ===========================================================
   DimCategories — SCD1 + Delete Flag
   =========================================================== */

CREATE TABLE DimCategories (
    Category_SK INT IDENTITY(1,1) PRIMARY KEY,

    Category_NK INT NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED BY ASSIGNMENT

    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX),

    IsDeleted BIT DEFAULT 0,

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimCustomers — SCD2 (Historical)
   =========================================================== */

CREATE TABLE DimCustomers (
    Customer_SK INT IDENTITY(1,1) PRIMARY KEY,

    Customer_NK NVARCHAR(50) NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(50),
    Country NVARCHAR(255),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),

    ValidFrom DATETIME NOT NULL,
    ValidTo DATETIME NOT NULL,
    IsCurrent BIT NOT NULL,

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimEmployees — SCD1 + Delete Flag
   =========================================================== */

CREATE TABLE DimEmployees (
    Employee_SK INT IDENTITY(1,1) PRIMARY KEY,

    Employee_NK INT NOT NULL,      
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    LastName NVARCHAR(255),
    FirstName NVARCHAR(255),
    Title NVARCHAR(255),
    TitleOfCourtesy NVARCHAR(50),
    BirthDate NVARCHAR(50),
    HireDate NVARCHAR(50),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(50),
    Country NVARCHAR(255),
    HomePhone NVARCHAR(50),
    Extension NVARCHAR(10),
    Notes NVARCHAR(MAX),
    ReportsTo INT,

    IsDeleted BIT DEFAULT 0,

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimSuppliers — SCD4 Dimension
   =========================================================== */

CREATE TABLE DimSuppliers (
    Supplier_SK INT IDENTITY(1,1) PRIMARY KEY,

    Supplier_NK INT NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(50),
    Country NVARCHAR(255),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),
    HomePage NVARCHAR(MAX),

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimProducts — SCD2 (with Closing)
   =========================================================== */

CREATE TABLE DimProducts (
    Product_SK INT IDENTITY(1,1) PRIMARY KEY,

    Product_NK INT NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    ProductName NVARCHAR(255),
    Supplier_NK INT,
    Category_NK INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice FLOAT,
    UnitsInStock INT,
    UnitsOnOrder INT,
    ReorderLevel INT,
    Discontinued NVARCHAR(10),

    ValidFrom DATETIME NOT NULL,
    ValidTo DATETIME NOT NULL,
    IsCurrent BIT NOT NULL,
    IsClosed BIT DEFAULT 0,

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimRegion — SCD1
   =========================================================== */

CREATE TABLE DimRegion (
    Region_SK INT IDENTITY(1,1) PRIMARY KEY,

    Region_NK INT NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    RegionDescription NVARCHAR(255),
    RegionCategory NVARCHAR(255),
    RegionImportance NVARCHAR(255),

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimShippers — SCD1
   =========================================================== */

CREATE TABLE DimShippers (
    Shipper_SK INT IDENTITY(1,1) PRIMARY KEY,

    Shipper_NK INT NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    CompanyName NVARCHAR(255),
    Phone NVARCHAR(50),

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   DimTerritories — SCD3
   =========================================================== */

CREATE TABLE DimTerritories (
    Territory_SK INT IDENTITY(1,1) PRIMARY KEY,

    Territory_NK NVARCHAR(50) NOT NULL,
    staging_raw_id_sk INT NOT NULL,     -- REQUIRED

    TerritoryDescription_Current NVARCHAR(255),
    TerritoryDescription_Prior NVARCHAR(255),
    TerritoryCode NVARCHAR(10),
    Region_NK INT,

    SOR_SK INT,
    LoadDate DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK)
);


/* ===========================================================
   FACT ORDERS — SNAPSHOT FACT
   =========================================================== */

CREATE TABLE FactOrders (
    FactOrder_SK INT IDENTITY(1,1) PRIMARY KEY,

    Order_NK INT NOT NULL,
    Product_NK INT NOT NULL,

    Customer_SK INT,
    Employee_SK INT,
    Product_SK INT,
    Shipper_SK INT,
    Territory_SK INT,
    Region_SK INT,

    OrderDate NVARCHAR(50),
    RequiredDate NVARCHAR(50),
    ShippedDate NVARCHAR(50),
    Freight FLOAT,

    UnitPrice FLOAT,
    Quantity INT,
    Discount FLOAT,

    LoadDate DATETIME DEFAULT GETDATE(),

    SOR_SK INT,
    staging_raw_id_sk INT,

    -- FOREIGN KEYS
    FOREIGN KEY (SOR_SK) REFERENCES Dim_SOR(SOR_SK),
    FOREIGN KEY (Customer_SK) REFERENCES DimCustomers(Customer_SK),
    FOREIGN KEY (Employee_SK) REFERENCES DimEmployees(Employee_SK),
    FOREIGN KEY (Product_SK)  REFERENCES DimProducts(Product_SK),
    FOREIGN KEY (Shipper_SK)  REFERENCES DimShippers(Shipper_SK),
    FOREIGN KEY (Territory_SK) REFERENCES DimTerritories(Territory_SK),
    FOREIGN KEY (Region_SK)   REFERENCES DimRegion(Region_SK)
);