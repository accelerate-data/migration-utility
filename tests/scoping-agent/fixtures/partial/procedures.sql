-- Procedure uses only dynamic SQL — static analysis cannot confirm what it writes
-- Confidence should be capped at 0.45 (partial)
CREATE PROCEDURE [silver].[usp_load_DimChannel]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @tableName NVARCHAR(200) = N'[silver].[DimChannel]';

    SET @sql = N'TRUNCATE TABLE ' + @tableName;
    EXEC sp_executesql @sql;

    SET @sql = N'INSERT INTO ' + @tableName + N' ([ChannelKey], [ChannelCode], [ChannelName])
                 SELECT [ChannelID], [Code], [Name] FROM [bronze].[Channel]';
    EXEC sp_executesql @sql;
END
GO
