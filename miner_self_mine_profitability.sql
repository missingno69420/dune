-- Analyze validator profitability from task fees, validator rewards, and fees within a lookback period
-- Include task fee, validator rewards, validator fees, net profit, task metadata, and model names for tasks
-- Convert token values to decimal AIUS (wei / 10^18) in output
-- Order by evt_block_number, evt_tx_index, evt_index to reflect on-chain submission order
-- Use query_5179596 for RewardsPaid and FeesPaid with task_id assignments
-- Use LEFT JOIN for solutions and payouts to include tasks without events
-- Remove time filters in solutions/payouts as joins ensure post-task events
-- Join with query_5169304 for model names using model_id
WITH tasks AS (
  SELECT
    id AS task_id,
    evt_tx_hash AS tx_hash,
    fee AS task_fee, -- Task fee in raw AIUS wei
    evt_block_number AS task_block_number, -- Task submission block number
    evt_tx_index, -- Transaction index within the block
    evt_index, -- Log index within the transaction
    model as model_id -- Model ID for joining with query_5169304
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  WHERE evt_block_time >= NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE -- Lookback period (default 1440 minutes)
),
solutions AS (
  SELECT
    s.task AS task_id,
    s.evt_tx_hash AS tx_hash
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed s
  LEFT JOIN tasks t ON s.task = t.task_id -- Include solutions for tasks in lookback
),
payouts AS (
  SELECT
    p.task_id,
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_rewards, -- Sum validator rewards in raw AIUS wei
    SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fees -- Sum validator fees in raw AIUS wei
  FROM query_5179596 p
  LEFT JOIN tasks t ON p.task_id = t.task_id -- Include payouts for tasks in lookback
  WHERE p.task_id IS NOT NULL -- Ensure non-null task_id
  GROUP BY p.task_id
),
task_summary AS (
  -- Combine tasks, solutions, payouts, and model names
  SELECT
    t.task_id,
    t.task_block_number, -- Task submission block number
    t.evt_tx_index, -- Transaction index within the block
    t.evt_index, -- Log index within the transaction
    COALESCE(t.task_fee, 0) AS task_fee, -- Task fee in raw AIUS wei
    COALESCE(p.validator_rewards, 0) AS validator_rewards, -- Validator rewards in raw AIUS wei
    COALESCE(p.validator_fees, 0) AS validator_fees, -- Validator fees in raw AIUS wei
    CAST(COALESCE(p.validator_rewards, 0) AS DECIMAL(38, 0)) +
    CAST(COALESCE(p.validator_fees, 0) AS DECIMAL(38, 0)) -
    CAST(COALESCE(t.task_fee, 0) AS DECIMAL(38, 0)) AS profit_aius_tokens, -- Net profit in raw AIUS wei (allows negative)
    m.model_name, -- Model name from query_5169304
    t.model_id
  FROM tasks t
  LEFT JOIN solutions s ON t.task_id = s.task_id -- Include tasks even without solutions
  LEFT JOIN payouts p ON t.task_id = p.task_id -- Include tasks even without payouts
  LEFT JOIN query_5169304 m ON t.model_id = m.model_id -- Include model names, allowing NULL if no match
)
-- Output results with decimal AIUS tokens and model names, ordered by on-chain submission order
SELECT
  task_id,
  task_block_number,
  COALESCE(model_name, to_hex(model_id), 'No Task') as model, -- model_name, -- Model name from query_5169304
  CAST(CAST(task_fee AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS task_fee_aius, -- Task fee in decimal AIUS
  CAST(CAST(validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_rewards_aius, -- Validator rewards in decimal AIUS
  CAST(CAST(validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_fees_aius, -- Validator fees in decimal AIUS
  CAST(CAST(profit_aius_tokens AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS profit_aius_tokens -- Net profit in decimal AIUS
FROM task_summary
ORDER BY task_block_number, evt_tx_index, evt_index -- Order by on-chain submission sequence
