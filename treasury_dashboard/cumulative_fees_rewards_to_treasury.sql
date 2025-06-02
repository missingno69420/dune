-- https://dune.com/queries/5220025/
WITH fees_paid AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(treasuryFee + (remainingFee - validatorFee)) AS treasury_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
  GROUP BY 1
),
rewards_paid AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(treasuryReward) AS treasury_reward
  FROM arbius_arbitrum.v2_enginev5_1_evt_rewardspaid
  GROUP BY 1
),
combined AS (
  SELECT
    day,
    COALESCE(SUM(treasury_fee), 0) AS treasury_fee,
    COALESCE(SUM(treasury_reward), 0) AS treasury_reward
  FROM (
    SELECT day, treasury_fee, 0 AS treasury_reward FROM fees_paid
    UNION ALL
    SELECT day, 0 AS treasury_fee, treasury_reward FROM rewards_paid
  ) sub
  GROUP BY day
),
cumulative_sums AS (
  SELECT
    day,
    SUM(treasury_fee) OVER (ORDER BY day) AS cumulative_treasury_fee,
    SUM(treasury_reward) OVER (ORDER BY day) AS cumulative_treasury_reward,
    SUM(treasury_fee + treasury_reward) OVER (ORDER BY day) AS cumulative_total_treasury
  FROM combined
)
SELECT
  day,
  cumulative_treasury_fee / 1e18 AS cumulative_treasury_fee_tokens,
  cumulative_treasury_reward / 1e18 AS cumulative_treasury_reward_tokens,
  cumulative_total_treasury / 1e18 AS cumulative_total_treasury_tokens
FROM cumulative_sums
ORDER BY day
