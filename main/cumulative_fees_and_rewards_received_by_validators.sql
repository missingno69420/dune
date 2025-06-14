-- https://dune.com/queries/5282894/
WITH fees_paid_to_validators AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(validatorFee) AS validator_fee
  FROM arbius_arbitrum.engine_evt_feespaid
  GROUP BY 1
),
rewards_paid_to_validators AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(validatorReward) AS validator_reward
  FROM arbius_arbitrum.engine_evt_rewardspaid
  GROUP BY 1
),
combined AS (
  SELECT
    day,
    COALESCE(SUM(validator_fee), 0) AS validator_fee,
    COALESCE(SUM(validator_reward), 0) AS validator_reward
  FROM (
    SELECT day, validator_fee, 0 AS validator_reward FROM fees_paid_to_validators
    UNION ALL
    SELECT day, 0 AS validator_fee, validator_reward FROM rewards_paid_to_validators
  ) sub
  GROUP BY day
),
cumulative_sums AS (
  SELECT
    day,
    SUM(validator_fee) OVER (ORDER BY day) AS cumulative_validator_fee,
    SUM(validator_reward) OVER (ORDER BY day) AS cumulative_validator_reward,
    SUM(validator_fee + validator_reward) OVER (ORDER BY day) AS cumulative_total_to_validators
  FROM combined
)
SELECT
  day,
  cumulative_validator_fee / 1e18 AS cumulative_validator_fee_tokens,
  cumulative_validator_reward / 1e18 AS cumulative_validator_reward_tokens,
  cumulative_total_to_validators / 1e18 AS cumulative_total_to_validators_tokens
FROM cumulative_sums
ORDER BY day DESC;
