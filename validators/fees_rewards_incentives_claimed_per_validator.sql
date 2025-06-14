-- https://dune.com/queries/5285077/
WITH rewards_and_fees AS (
  SELECT
    block_time,
    task_id,
    validator_reward,
    validator_fee
  FROM query_5179596
  WHERE task_id IS NOT NULL
),
solutions AS (
  SELECT
    addr AS validator_address,
    task
  FROM arbius_arbitrum.engine_evt_solutionsubmitted
),
combined AS (
  SELECT
    DATE(r.block_time) AS day,
    s.validator_address,
    COALESCE(r.validator_reward, 0) AS reward_amount,
    COALESCE(r.validator_fee, 0) AS fee_amount
  FROM rewards_and_fees r
  JOIN solutions s ON r.task_id = s.task
),
daily_rewards AS (
  SELECT
    day,
    validator_address,
    SUM(reward_amount) AS total_rewards_wei,
    SUM(fee_amount) AS total_fees_wei
  FROM combined
  GROUP BY day, validator_address
),
incentives AS (
  SELECT
    DATE(evt_block_time) AS day,
    recipient AS validator_address,
    SUM(amount) AS total_incentives_wei
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed
  GROUP BY DATE(evt_block_time), recipient
),
daily_solutions AS (
  SELECT
    evt_block_date AS day,
    addr AS validator_address,
    COUNT(*) AS solutions_submitted
  FROM arbius_arbitrum.engine_evt_solutionsubmitted
  GROUP BY evt_block_date, addr
),
all_days_validators AS (
  SELECT day, validator_address FROM daily_rewards
  UNION
  SELECT day, validator_address FROM incentives
),
final AS (
  SELECT
    a.day,
    a.validator_address,
    COALESCE(d.total_rewards_wei, 0) AS total_rewards_wei,
    COALESCE(d.total_fees_wei, 0) AS total_fees_wei,
    COALESCE(i.total_incentives_wei, 0) AS total_incentives_wei,
    COALESCE(ds.solutions_submitted, 0) AS solutions_submitted
  FROM all_days_validators a
  LEFT JOIN daily_rewards d ON a.day = d.day AND a.validator_address = d.validator_address
  LEFT JOIN incentives i ON a.day = i.day AND a.validator_address = i.validator_address
  LEFT JOIN daily_solutions ds ON a.day = ds.day AND a.validator_address = ds.validator_address
)
SELECT
  day,
  validator_address,
  CAST(CAST(total_rewards_wei AS DOUBLE) / 1e18 AS DECIMAL(38,18)) AS total_rewards_aius,
  CAST(CAST(total_fees_wei AS DOUBLE) / 1e18 AS DECIMAL(38,18)) AS total_fees_aius,
  CAST(CAST(total_incentives_wei AS DOUBLE) / 1e18 AS DECIMAL(38,18)) AS total_incentives_aius,
  CAST(CAST(total_rewards_wei + total_fees_wei + total_incentives_wei AS DOUBLE) / 1e18 AS DECIMAL(38,18)) AS total_earnings_aius,
  solutions_submitted
FROM final
ORDER BY day, total_earnings_aius DESC;
