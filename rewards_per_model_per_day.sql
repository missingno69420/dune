-- https://dune.com/queries/5176093/8520221
-- Use query_5179596 for RewardsPaid and FeesPaid events with pre-assigned task IDs
WITH rewards_fees_with_tasks AS (
  SELECT
    date_trunc('day', block_time) AS day,
    tx_hash,
    index,
    event_type,
    treasury_reward / POWER(10, 18) AS treasury_reward, -- Convert from wei to AIUS tokens
    task_owner_reward / POWER(10, 18) AS task_owner_reward,
    validator_reward / POWER(10, 18) AS validator_reward,
    model_owner_fee / POWER(10, 18) AS model_owner_fee,
    treasury_total_fee / POWER(10, 18) AS treasury_total_fee,
    validator_fee / POWER(10, 18) AS validator_fee,
    task_id
  FROM query_5179596
  WHERE event_type IN ('RewardsPaid', 'FeesPaid')
    AND task_id IS NOT NULL -- Ensure task_id is assigned
),
-- Get model_id from TaskSubmitted for linking to model names
task_models AS (
  SELECT
    id AS task_id,
    model AS model_id -- Model hash (varbinary)
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
),
-- Convert model_id to varchar with to_hex for joining with query_5169304
rewards_fees_with_models AS (
  SELECT
    r.day,
    to_hex(m.model_id) AS model_id, -- Convert varbinary to varchar
    r.treasury_reward,
    r.task_owner_reward,
    r.validator_reward,
    r.model_owner_fee,
    r.treasury_total_fee,
    r.validator_fee
  FROM rewards_fees_with_tasks r
  LEFT JOIN task_models m ON r.task_id = m.task_id
),
-- Aggregate rewards and fees per model per day, joining with query_5169304 for model names
daily_totals AS (
  SELECT
    day,
    COALESCE(m.model_name, r.model_id, 'No Task') AS model, -- Use model_name, fall back to model_id or 'No Task'
    SUM(r.treasury_reward) AS treasury_reward, -- Sum adjusted token values
    SUM(r.task_owner_reward) AS task_owner_reward,
    SUM(r.validator_reward) AS validator_reward,
    SUM(r.model_owner_fee) AS model_owner_fee_tokens,
    SUM(r.treasury_total_fee) AS treasury_fee_tokens,
    SUM(r.validator_fee) AS validator_fee_tokens
  FROM rewards_fees_with_models r
  LEFT JOIN query_5169304 m ON r.model_id = to_hex(m.model_id) -- Match varchar model_id
  GROUP BY day, COALESCE(m.model_name, r.model_id, 'No Task')
)
-- Final output with COALESCE to handle NULLs, ordered by day and model
SELECT
  day,
  model,
  COALESCE(treasury_reward, 0) AS treasury_reward,
  COALESCE(task_owner_reward, 0) AS task_owner_reward,
  COALESCE(validator_reward, 0) AS validator_reward,
  COALESCE(model_owner_fee_tokens, 0) AS model_owner_fee_tokens,
  COALESCE(treasury_fee_tokens, 0) AS treasury_fee_tokens,
  COALESCE(validator_fee_tokens, 0) AS validator_fee_tokens
FROM daily_totals
ORDER BY day, model
