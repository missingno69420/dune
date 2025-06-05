-- https://dune.com/queries/5228567
WITH daily_ve_rewards AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(totalRewards) AS ve_rewards
  FROM arbius_arbitrum.engine_evt_rewardspaid
  GROUP BY 1
),
cumulative_ve_rewards AS (
  SELECT
    day,
    SUM(ve_rewards) OVER (ORDER BY day) AS cumulative_ve_rewards
  FROM daily_ve_rewards
)
SELECT
  day,
  cumulative_ve_rewards / 1e18 AS cumulative_ve_rewards_tokens
FROM cumulative_ve_rewards
ORDER BY day
