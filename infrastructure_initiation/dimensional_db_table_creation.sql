USE ORDER_DDS;
GO

DROP TABLE IF EXISTS Dim_SOR;
DROP TABLE IF EXISTS DimCategories;
DROP TABLE IF EXISTS DimCustomers;
DROP TABLE IF EXISTS DimEmployees;
DROP TABLE IF EXISTS DimSuppliers;
DROP TABLE IF EXISTS DimSuppliers_History;
DROP TABLE IF EXISTS DimProducts;
DROP TABLE IF EXISTS DimRegion;
DROP TABLE IF EXISTS DimShippers;
DROP TABLE IF EXISTS DimTerritories;
DROP TABLE IF EXISTS FactOrders;
GO


CREATE TABLE Dim_SOR (
    SOR_SK INT IDENTITY(1,1) PRIMARY KEY,
    SourceTableName NVARCHAR(255) NOT NULL
);
GO

CREATE TABLE DimCategories (
    Category_SK INT IDENTITY(1,1) PRIMARY KEY,
    Category_NK INT NOT NULL,
    CategoryID INT,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX),
    IsDeleted BIT DEFAULT 0,
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO

CREATE TABLE DimCustomers (
    Customer_SK INT IDENTITY(1,1) PRIMARY KEY,
    Customer_NK INT NOT NULL,
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
    Fax NVARCHAR(50),
    ValidFrom DATETIME NOT NULL DEFAULT GETDATE(),
    ValidTo DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO

CREATE TABLE DimEmployees (
    Employee_SK INT IDENTITY(1,1) PRIMARY KEY,
    Empployee_NK INT NOT NULL,
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
    ReportsTo_FK INT,
    PhotoPath NVARCHAR(255),
    IsDeleted BIT DEFAULT 0,
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK),
    FOREIGN KEY (ReportsTo_FK) REFERENCES DimEmployees(Employee_SK) ON DELETE NO ACTION ON UPDATE NO ACTION
);
GO

CREATE TABLE DimSuppliers (
    Supplier_SK INT IDENTITY(1,1) PRIMARY KEY,
    Supplier_NK INT NOT NULL,
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
    HomePage NVARCHAR(MAX),
    SOR_FK INT,
    ValidFrom DATETIME,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO

CREATE TABLE DimSuppliers_History (
    Supplier_History_SK INT IDENTITY(1,1) PRIMARY KEY,
    Supplier_NK INT NOT NULL,
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
    HomePage NVARCHAR(MAX),
    SOR_FK INT,
    ValidFrom DATETIME NOT NULL,
    ValidTo DATETIME NULL,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO

CREATE TABLE DimProducts (
    Product_SK INT IDENTITY(1,1) PRIMARY KEY,
    Product_NK INT NOT NULL,
    ProductID INT,
    ProductName NVARCHAR(255),
    Supplier_FK INT,
    Category_FK INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice DECIMAL(10,2),
    UnitsInStock INT,
    UnitsOnOrder INT,
    ReorderLevel INT,
    Discontinued BIT,
    ValidFrom DATETIME NOT NULL DEFAULT GETDATE(),
    ValidTo DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    IsDeleted BIT DEFAULT 0,
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK),
    FOREIGN KEY (Supplier_FK) REFERENCES DimSuppliers(Supplier_SK) ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (Category_FK) REFERENCES DimCategories(Category_SK) ON DELETE CASCADE ON UPDATE CASCADE
);
GO

CREATE TABLE DimRegion (
    Region_SK INT IDENTITY(1,1) PRIMARY KEY,
    Region_NK INT NOT NULL,
    RegionID INT,
    RegionDescription NVARCHAR(255),
    RegionCategory NVARCHAR(255),
    RegionImportance NVARCHAR(255),
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO

CREATE TABLE DimShippers (
    Shipper_SK INT IDENTITY(1,1) PRIMARY KEY,
    Shipper_NK INT NOT NULL,
    ShipperID INT,
    CompanyName NVARCHAR(255),
    Phone NVARCHAR(50),
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO

CREATE TABLE DimTerritories (
    Territory_SK INT IDENTITY(1,1) PRIMARY KEY,
    Territory_NK INT NOT NULL,
    TerritoryID NVARCHAR(50),
    TerritoryDescription_Current NVARCHAR(255),
    TerritoryDescription_Prior NVARCHAR(255),
    TerritoryDescription_Prior_ValidTo DATETIME, 
    TerritoryCode NVARCHAR(255),
    Region_FK INT,
    SOR_FK INT,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK),
    FOREIGN KEY (Region_FK) REFERENCES DimRegion(Region_SK) ON DELETE CASCADE ON UPDATE CASCADE
);
GO

CREATE TABLE FactOrders (
    Order_SK INT IDENTITY(1,1) PRIMARY KEY,
    Order_NK INT NOT NULL,
    OrderID INT,
    Customer_FK INT,
    Employee_FK INT,
    Shipper_FK INT,
    Product_FK INT,
    Category_FK INT,
    Supplier_FK INT,
    Territory_FK INT,
    OrderDate DATETIME,
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    UnitPrice DECIMAL(10,2),
    Quantity INT,
    Discount FLOAT,
    Freight DECIMAL(10,2),
    SOR_FK INT,
    FOREIGN KEY (Customer_FK) REFERENCES DimCustomers(Customer_SK) ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (Employee_FK) REFERENCES DimEmployees(Employee_SK) ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (Shipper_FK) REFERENCES DimShippers(Shipper_SK) ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (Product_FK) REFERENCES DimProducts(Product_SK) ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (Territory_FK) REFERENCES DimTerritories(Territory_SK) ON DELETE NO ACTION ON UPDATE NO ACTION,
    FOREIGN KEY (SOR_FK) REFERENCES Dim_SOR(SOR_SK)
);
GO