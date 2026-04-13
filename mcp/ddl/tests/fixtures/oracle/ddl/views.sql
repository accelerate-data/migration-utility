CREATE OR REPLACE VIEW SH.PROFITS AS
SELECT s.channel_id, s.cust_id, s.prod_id
FROM SH.SALES s
GO
