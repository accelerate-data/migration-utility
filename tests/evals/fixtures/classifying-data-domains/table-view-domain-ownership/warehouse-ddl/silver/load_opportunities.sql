CREATE PROCEDURE silver.load_opportunities
AS
BEGIN
  SELECT opportunity_id
  FROM silver.opportunities;
END;
