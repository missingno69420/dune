-- https://dune.com/queries/5169801/
WITH fees_paid AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    (modelFee - treasuryFee) / 1e18 AS model_owner_fee,
    (treasuryFee + (remainingFee - validatorFee)) / 1e18 AS treasury_fee,
    validatorFee / 1e18 AS validators_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
),
daily_totals AS (
  SELECT
    day,
    SUM(model_owner_fee) AS model_owner_fee_tokens,
    SUM(treasury_fee) AS treasury_fee_tokens,
    SUM(validators_fee) AS validators_fee_tokens
  FROM fees_paid
  GROUP BY day
)
SELECT
  day,
  COALESCE(SUM(model_owner_fee_tokens) OVER (ORDER BY day), 0) AS cumulative_model_owner_fee_tokens,
  COALESCE(SUM(treasury_fee_tokens) OVER (ORDER BY day), 0) AS cumulative_treasury_fee_tokens,
  COALESCE(SUM(validators_fee_tokens) OVER (ORDER BY day), 0) AS cumulative_validators_fee_tokens
FROM daily_totals
ORDER BY day
