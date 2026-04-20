CREATE FUNCTION gold.opportunity_value(@amount numeric(18, 2))
RETURNS numeric(18, 2)
AS
BEGIN
  RETURN @amount;
END;
