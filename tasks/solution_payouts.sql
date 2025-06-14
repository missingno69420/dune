-- https://dune.com/queries/5284860/
WITH payouts AS (
  SELECT
    MAX(CASE WHEN event_type = 'FeesPaid' THEN model_owner_fee END) AS model_owner_fee,
    MAX(CASE WHEN event_type = 'FeesPaid' THEN treasury_total_fee END) AS treasury_total_fee,
    MAX(CASE WHEN event_type = 'FeesPaid' THEN validator_fee END) AS validator_fee,
    MAX(CASE WHEN event_type = 'RewardsPaid' THEN treasury_reward END) AS treasury_reward,
    MAX(CASE WHEN event_type = 'RewardsPaid' THEN task_owner_reward END) AS task_owner_reward,
    MAX(CASE WHEN event_type = 'RewardsPaid' THEN validator_reward END) AS validator_reward
  FROM query_5179596
  WHERE task_id = {{task_id}}
),
incentive_paid AS (
  SELECT COALESCE(amount, 0) AS incentive_paid
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed
  WHERE taskid = {{task_id}}
)
SELECT 'Model Owner Fee' AS type,
       COALESCE(CAST(CAST(model_owner_fee AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM payouts
UNION ALL
SELECT 'Treasury Fee' AS type,
       COALESCE(CAST(CAST(treasury_total_fee AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM payouts
UNION ALL
SELECT 'Validator Fee' AS type,
       COALESCE(CAST(CAST(validator_fee AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM payouts
UNION ALL
SELECT 'Treasury Reward' AS type,
       COALESCE(CAST(CAST(treasury_reward AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM payouts
UNION ALL
SELECT 'Task Owner Reward' AS type,
       COALESCE(CAST(CAST(task_owner_reward AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM payouts
UNION ALL
SELECT 'Validator Reward' AS type,
       COALESCE(CAST(CAST(validator_reward AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM payouts
UNION ALL
SELECT 'Incentive Paid' AS type,
       COALESCE(CAST(CAST(incentive_paid AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM incentive_paid
