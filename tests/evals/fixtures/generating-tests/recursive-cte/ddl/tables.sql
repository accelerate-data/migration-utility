-- ============================================================
-- SOURCE: bronze.Employee (simplified test schema)
-- ============================================================
CREATE TABLE bronze.Employee (
    EmployeeID INT NOT NULL PRIMARY KEY,
    BusinessEntityID INT NOT NULL,
    NationalIDNumber NVARCHAR(15) NOT NULL,
    LoginID NVARCHAR(256) NOT NULL,
    JobTitle NVARCHAR(50) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus NCHAR(1) NOT NULL,
    Gender NCHAR(1) NOT NULL,
    HireDate DATE NOT NULL,
    SalariedFlag BIT NOT NULL,
    VacationHours SMALLINT NOT NULL,
    SickLeaveHours SMALLINT NOT NULL,
    CurrentFlag BIT NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    ManagerID INT NULL,
    ModifiedDate DATETIME NOT NULL
);

-- ============================================================
-- TARGET: silver.RecursiveCteTarget
-- ============================================================
CREATE TABLE silver.RecursiveCteTarget (
    OrgLevel INT NOT NULL,
    ManagerId INT NULL,
    EmployeeId INT NOT NULL PRIMARY KEY,
    FullPath NVARCHAR(100) NOT NULL
);

GO
