CREATE PROCEDURE dbo.usp_cycle_a
AS BEGIN
    EXEC dbo.usp_cycle_b
END
GO
CREATE PROCEDURE dbo.usp_cycle_b
AS BEGIN
    EXEC dbo.usp_cycle_a
END
GO
