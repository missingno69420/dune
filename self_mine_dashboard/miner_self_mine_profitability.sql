WITH tasks AS (
  SELECT
    id AS task_id,
    evt_tx_hash AS task_tx_hash, -- Transaction ID for TaskSubmitted
    fee AS task_fee, -- Task fee in raw AIUS wei
    evt_block_number AS task_block_number, -- Task submission block number
    evt_tx_index, -- Transaction index within the block
    evt_index, -- Log index within the transaction
    model AS model_id -- Model ID for joining with query_5169304
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  WHERE evt_block_time >= NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE -- Lookback period (default 1440 minutes)
),
payouts AS (
  SELECT
    p.task_id,
    p.tx_hash AS payout_tx_hash, -- Transaction ID for RewardsPaid and FeesPaid
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_rewards, -- Sum validator rewards in raw AIUS wei
    SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fees -- Sum validator fees in raw AIUS wei
  FROM query_5179596 p
  WHERE p.task_id IS NOT NULL -- Ensure non-null task_id
  GROUP BY p.task_id, p.tx_hash
),
task_summary AS (
  -- Combine tasks, solutions, payouts, and model names
  SELECT
    t.task_id,
    t.task_tx_hash, -- Transaction ID for TaskSubmitted
    p.payout_tx_hash, -- Transaction ID for SolutionClaimed or ContestationVoteFinish
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
  JOIN payouts p ON t.task_id = p.task_id -- Restrict to tasks even with payouts
  LEFT JOIN query_5169304 m ON t.model_id = m.model_id -- Include model names, allowing NULL if no match
),
indexed_tasks AS (
  -- Add global index
  SELECT
    task_id,
    task_tx_hash,
    payout_tx_hash,
    task_block_number,
    evt_tx_index,
    evt_index,
    COALESCE(model_name, to_hex(model_id), 'No Task') AS model,
    -1 * CAST(CAST(task_fee AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS task_fee_aius, -- Task fee in decimal AIUS
    CAST(CAST(validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_rewards_aius, -- Validator rewards in decimal AIUS
    CAST(CAST(validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_fees_aius, -- Validator fees in decimal AIUS
    CAST(CAST(profit_aius_tokens AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS profit_aius_tokens, -- Net profit in decimal AIUS
    ROW_NUMBER() OVER (ORDER BY task_block_number ASC, evt_tx_index DESC, evt_index DESC) AS global_index -- Global index with most recent task having highest index
  FROM task_summary
)
-- Output results with transaction IDs, decimal AIUS tokens, model names, and global index, ordered by chronological submission order
SELECT
  task_id,
  task_tx_hash, -- Transaction ID for TaskSubmitted
  payout_tx_hash, -- Transaction ID for SolutionClaimed or ContestationVoteFinish
  task_block_number,
  model,
  task_fee_aius,
  validator_rewards_aius,
  validator_fees_aius,
  profit_aius_tokens,
  global_index -- Global order index
FROM indexed_tasks
ORDER BY task_block_number ASC, evt_tx_index DESC, evt_index DESC -- Order by chronological submission sequence
