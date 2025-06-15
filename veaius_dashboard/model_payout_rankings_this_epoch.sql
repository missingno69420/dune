WITH params AS (
  SELECT
    DATE_TRUNC('day', DATE_TRUNC('week', CURRENT_TIMESTAMP) + INTERVAL '3' day) AS start_time -- Thursday 00:00:00 UTC
),
tasks AS (
  -- Fetch tasks submitted since last Thursday
  SELECT
    id AS task_id,
    evt_tx_hash AS task_tx_hash,
    fee AS task_fee,
    evt_block_number AS task_block_number,
    evt_tx_index,
    evt_index,
    model AS model_id
  FROM arbius_arbitrum.engine_evt_tasksubmitted
  CROSS JOIN params p
  WHERE evt_block_time >= p.start_time
),
payouts AS (
  -- Aggregate validator rewards and fees only for transactions with a SolutionClaimed event for the task
  SELECT
    p.task_id,
    p.tx_hash AS payout_tx_hash,
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_rewards,
    SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fees
  FROM query_5179596 p
  JOIN (
    SELECT tx_hash, task_id
    FROM query_5179596
    WHERE event_type = 'SolutionClaimed'
  ) sc ON p.tx_hash = sc.tx_hash AND p.task_id = sc.task_id
  WHERE p.task_id IS NOT NULL
  GROUP BY p.task_id, p.tx_hash
),
task_summary AS (
  -- Combine tasks with filtered payouts and calculate total payout
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
    CAST(COALESCE(p.validator_rewards, 0) AS DECIMAL(38, 0)) +
    CAST(COALESCE(p.validator_fees, 0) AS DECIMAL(38, 0)) AS total_payout,
    m.model_name,
    t.model_id
  FROM tasks t
  JOIN payouts p ON t.task_id = p.task_id
  LEFT JOIN query_5169304 m ON t.model_id = m.model_id
),
ranked_tasks AS (
  -- Rank tasks per model by total payout, lowest first
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY total_payout ASC) AS rn
  FROM task_summary
)
-- Select the task with the lowest payout per model and format output
SELECT
  task_id,
  task_tx_hash,
  payout_tx_hash,
  task_block_number,
  COALESCE(model_name, to_hex(model_id), 'No Task') AS model,
  CAST(CAST(task_fee AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS task_fee_aius,
  CAST(CAST(validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_rewards_aius,
  CAST(CAST(validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_fees_aius,
  CAST(CAST(total_payout AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS total_payout_aius
FROM ranked_tasks
WHERE rn = 1
ORDER BY model
