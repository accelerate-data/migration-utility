CREATE TABLE [silver].[DimEmployee]
(
    [EmployeeKey] INT NOT NULL,
    [FirstName] NVARCHAR(100) NOT NULL,
    [LastName] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_silver_DimEmployee] PRIMARY KEY ([EmployeeKey])
)
GO
