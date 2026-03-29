-- Good proc
CREATE PROCEDURE dbo.usp_Good
AS
BEGIN
    INSERT INTO dbo.Target (Col) SELECT Col FROM dbo.Source;
END
GO
-- Bad proc: totally invalid SQL
CREATE PROCEDURE dbo.usp_Bad
AS
BEGIN
    THIS IS NOT VALID SQL AT ALL GARBAGE;
END
GO
