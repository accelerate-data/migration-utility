CREATE VIEW gold.sold_opportunities AS
SELECT
  opportunity_id,
  account_id,
  sales_rep_id,
  amount,
  created_at
FROM silver.opportunities
WHERE stage_name = 'Closed Won';
