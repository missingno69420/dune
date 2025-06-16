-- https://dune.com/queries/5181373
WITH tasks AS (
  SELECT
    id AS task_id,
    evt_tx_hash AS task_tx_hash,
    fee AS task_fee,
    evt_block_number AS task_block_number,
    evt_tx_index,
    evt_index,
    model AS model_id
  FROM arbius_arbitrum.engine_evt_tasksubmitted
  WHERE evt_block_time >= NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE
    AND model = from_hex(replace('{{model_id}}', '0x', ''))
),
payouts AS (
  SELECT
    p.task_id,
    p.tx_hash AS payout_tx_hash,
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_rewards,
    SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fees,
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.task_owner_reward ELSE 0 END) AS task_owner_rewards
  FROM query_5179596 p
  WHERE p.task_id IS NOT NULL
  GROUP BY p.task_id, p.tx_hash
),
task_summary AS (
  SELECT
    t.task_id,
    t.task_tx_hash,
    p.payout_tx_hash,
    t.task_block_number,
    t.evt_tx_index,
    t.evt_index,
    COALESCE(t.task_fee, 0) AS task_fee,
    COALESCE(p.validator_rewards, 0) AS validator_rewards,
    COALESCE(p.validator_fees, 0) AS validator_fees,
    COALESCE(p.task_owner_rewards, 0) AS task_owner_rewards,
    CAST(COALESCE(p.validator_rewards, 0) AS DECIMAL(38, 0)) +
    CAST(COALESCE(p.task_owner_rewards, 0) AS DECIMAL(38, 0)) +
    CAST(COALESCE(p.validator_fees, 0) AS DECIMAL(38, 0)) -
    CAST(COALESCE(t.task_fee, 0) AS DECIMAL(38, 0)) AS profit_aius_tokens,
    m.model_name,
    t.model_id
  FROM tasks t
  JOIN payouts p ON t.task_id = p.task_id
  LEFT JOIN query_5169304 m ON t.model_id = m.model_id
),
indexed_tasks AS (
  SELECT
    task_id,
    task_tx_hash,
    payout_tx_hash,
    task_block_number,
    evt_tx_index,
    evt_index,
    COALESCE(model_name, to_hex(model_id), 'No Task') AS model,
    -1 * CAST(CAST(task_fee AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS task_fee_aius,
    CAST(CAST(validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_rewards_aius,
    CAST(CAST(validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_fees_aius,
    CAST(CAST(task_owner_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS task_owner_rewards_aius,
    CAST(CAST(profit_aius_tokens AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS profit_aius_tokens,
    ROW_NUMBER() OVER (ORDER BY task_block_number ASC, evt_tx_index ASC, evt_index ASC) AS global_index
  FROM task_summary
)
SELECT
  task_id,
  task_tx_hash,
  payout_tx_hash,
  task_block_number,
  model,
  task_fee_aius,
  validator_rewards_aius,
  validator_fees_aius,
  task_owner_rewards_aius,
  profit_aius_tokens,
  global_index
FROM indexed_tasks
ORDER BY task_block_number ASC, evt_tx_index ASC, evt_index ASC
