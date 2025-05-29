-- https://dune.com/queries/5169801/
WITH fees_paid AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    (modelFee - treasuryFee) AS model_owner_fee,
    (treasuryFee + (remainingFee - validatorFee)) AS treasury_fee,
    validatorFee AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
),
daily_totals AS (
  SELECT
    day,
    SUM(model_owner_fee) AS model_owner_fee_sum,
    SUM(treasury_fee) AS treasury_fee_sum,
    SUM(validator_fee) AS validator_fee_sum
  FROM fees_paid
  GROUP BY day
)
SELECT
  day,
  COALESCE(SUM(model_owner_fee_sum) OVER (ORDER BY day) / 1e18, 0) AS cumulative_model_owner_fee_tokens,
  COALESCE(SUM(treasury_fee_sum) OVER (ORDER BY day) / 1e18, 0) AS cumulative_treasury_fee_tokens,
  COALESCE(SUM(validator_fee_sum) OVER (ORDER BY day) / 1e18, 0) AS cumulative_validator_fee_tokens
FROM daily_totals
ORDER BY day
