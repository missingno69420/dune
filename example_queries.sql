-- Some example queries for your reference, grok!

-- fees paid rewards paid events with context
WITH all_events AS (
  SELECT
    evt_tx_hash AS tx_hash, -- Transaction hash for grouping events
    evt_index AS index, -- Event log index for ordering within a transaction
    evt_block_time AS block_time, -- Timestamp for filtering or aggregation
    'RewardsPaid' AS event_type, -- Label for RewardsPaid events
    NULL AS task_id, -- Placeholder for task ID (to be assigned later)
    treasuryReward AS treasury_reward, -- Reward to treasury (raw wei)
    taskOwnerReward AS task_owner_reward, -- Reward to task owner (raw wei)
    validatorReward AS validator_reward, -- Reward to validator (raw wei)
    NULL AS model_owner_fee, -- Placeholder for FeesPaid fields
    NULL AS treasury_total_fee, -- Placeholder for FeesPaid total fee
    NULL AS validator_fee -- Placeholder for FeesPaid validator fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_rewardspaid
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'FeesPaid' AS event_type,
    NULL AS task_id,
    NULL AS treasury_reward, -- Placeholder for RewardsPaid fields
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    (modelFee - treasuryFee) AS model_owner_fee, -- Fee to model owner (raw wei)
    (treasuryFee + (remainingFee - validatorFee)) AS treasury_total_fee, -- Total fee to treasury (raw wei)
    validatorFee AS validator_fee -- Validator fee (raw wei)
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'SolutionClaimed' AS event_type,
    task AS task_id, -- Native task ID for SolutionClaimed
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS model_owner_fee,
    NULL AS treasury_total_fee,
    NULL AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'ContestationVoteFinish' AS event_type,
    id AS task_id, -- Native task ID for ContestationVoteFinish
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS model_owner_fee,
    NULL AS treasury_total_fee,
    NULL AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_contestationvotefinish
),
-- Add context for task ID assignment by capturing next events and task IDs
events_with_context AS (
  SELECT
    *,
    LEAD(event_type) OVER (PARTITION BY tx_hash ORDER BY index) AS next_event_type, -- Next event for FeesPaid logic
    LEAD(event_type, 2) OVER (PARTITION BY tx_hash ORDER BY index) AS next_next_event_type, -- Event after next
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END) OVER (PARTITION BY tx_hash ORDER BY index) AS next_contestation_task_id, -- Next ContestationVoteFinish task ID
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END, 2) OVER (PARTITION BY tx_hash ORDER BY index) AS next_next_contestation_task_id -- ContestationVoteFinish task ID after next
  FROM all_events
),
-- Filter to RewardsPaid and FeesPaid events for task ID assignment
rewards_fees_events AS (
  SELECT
    tx_hash,
    index,
    block_time,
    event_type,
    treasury_reward,
    task_owner_reward,
    validator_reward,
    model_owner_fee,
    treasury_total_fee,
    validator_fee,
    next_event_type,
    next_next_event_type,
    next_contestation_task_id,
    next_next_contestation_task_id
  FROM events_with_context
  WHERE event_type IN ('RewardsPaid', 'FeesPaid')
),
-- Find the closest preceding FeesPaid event for RewardsPaid and FeesPaid events
fees_candidates AS (
  SELECT
    r.tx_hash,
    r.index AS rewards_fees_index, -- Index of RewardsPaid or FeesPaid
    r.event_type,
    f.evt_index AS fees_index, -- Index of FeesPaid
    ROW_NUMBER() OVER (PARTITION BY r.tx_hash, r.index ORDER BY f.evt_index DESC) AS fee_rank -- Rank by proximity (1 = closest)
  FROM rewards_fees_events r
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_feespaid f
    ON r.tx_hash = f.evt_tx_hash
    AND f.evt_index < r.index
  WHERE r.event_type = 'RewardsPaid'
    OR r.event_type = 'FeesPaid'
),
-- Find the closest preceding SolutionClaimed for each matched FeesPaid
solution_candidates AS (
  SELECT
    f.tx_hash,
    f.rewards_fees_index,
    f.event_type,
    f.fees_index,
    s.evt_index AS solution_index,
    s.task AS solution_task_id,
    ROW_NUMBER() OVER (PARTITION BY f.tx_hash, f.rewards_fees_index, f.fees_index ORDER BY s.evt_index DESC) AS solution_rank
  FROM fees_candidates f
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed s
    ON f.tx_hash = s.evt_tx_hash
    AND s.evt_index < f.fees_index
  WHERE f.fee_rank = 1
),
-- Find the closest preceding SolutionClaimed for FeesPaid events directly
solution_for_fees AS (
  SELECT
    r.tx_hash,
    r.index AS rewards_fees_index,
    r.event_type,
    s.evt_index AS solution_index,
    s.task AS solution_task_id,
    ROW_NUMBER() OVER (PARTITION BY r.tx_hash, r.index ORDER BY s.evt_index DESC) AS solution_rank
  FROM rewards_fees_events r
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed s
    ON r.tx_hash = s.evt_tx_hash
    AND s.evt_index < r.index
  WHERE r.event_type = 'FeesPaid'
),
-- Assign task IDs to all events
events_with_task_id AS (
  SELECT
    r.block_time,
    r.tx_hash,
    r.index,
    r.event_type,
    r.treasury_reward,
    r.task_owner_reward,
    r.validator_reward,
    r.model_owner_fee,
    r.treasury_total_fee,
    r.validator_fee,
    CASE
      WHEN r.event_type = 'FeesPaid' THEN
        CASE
          WHEN sf.solution_task_id IS NOT NULL THEN sf.solution_task_id
          WHEN r.next_event_type = 'ContestationVoteFinish' THEN r.next_contestation_task_id
          WHEN r.next_event_type = 'RewardsPaid' AND r.next_next_event_type = 'ContestationVoteFinish' THEN r.next_next_contestation_task_id
          ELSE NULL
        END
      WHEN r.event_type = 'RewardsPaid' THEN
        CASE
          WHEN s.solution_task_id IS NOT NULL THEN s.solution_task_id
          WHEN r.next_event_type = 'ContestationVoteFinish' THEN r.next_contestation_task_id
          ELSE NULL
        END
      WHEN r.event_type IN ('SolutionClaimed', 'ContestationVoteFinish') THEN r.task_id -- Use native task_id
      ELSE NULL
    END AS task_id
  FROM events_with_context r
  LEFT JOIN fees_candidates f
    ON r.tx_hash = f.tx_hash
    AND r.index = f.rewards_fees_index
    AND r.event_type = f.event_type
    AND f.fee_rank = 1
  LEFT JOIN solution_candidates s
    ON f.tx_hash = s.tx_hash
    AND f.rewards_fees_index = s.rewards_fees_index
    AND f.fees_index = s.fees_index
    AND f.event_type = s.event_type
    AND s.solution_rank = 1
  LEFT JOIN solution_for_fees sf
    ON r.tx_hash = sf.tx_hash
    AND r.index = sf.rewards_fees_index
    AND r.event_type = sf.event_type
    AND sf.solution_rank = 1
)
-- Select simplified columns with assigned task_id
SELECT
  block_time,
  tx_hash,
  index,
  event_type,
  treasury_reward,
  task_owner_reward,
  validator_reward,
  model_owner_fee,
  treasury_total_fee,
  validator_fee,
  task_id
FROM events_with_task_id
ORDER BY block_time, tx_hash, index


-- self mine profitability
-- Analyze validator profitability from task fees, validator rewards, and fees within a lookback period
-- Include task and solution transaction IDs, task fee, validator rewards, validator fees, net profit, task metadata, and model names
-- Restrict to tasks with SolutionClaimed events for relevant profitability analysis
-- Convert token values to decimal AIUS (wei / 10^18) in output
-- Order by evt_block_number, evt_tx_index, evt_index to reflect on-chain submission order
-- Use query_5179596 for RewardsPaid and FeesPaid with task_id assignments
-- Use INNER JOIN for solutions to ensure tasks have solutions, LEFT JOIN for payouts to allow missing payouts
-- Remove time filters in solutions/payouts as joins ensure post-task events
-- Join with query_5169304 for model names using model column
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
  -- LEFT JOIN tasks t ON p.task_id = t.task_id -- Include payouts for tasks in lookback, allowing missing payouts
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
)
-- Output results with transaction IDs, decimal AIUS tokens, and model names, ordered by on-chain submission order
SELECT
  task_id,
  task_tx_hash, -- Transaction ID for TaskSubmitted
  payout_tx_hash, -- Transaction ID for SolutionClaimed or ContestationVoteFinish
  task_block_number,
  COALESCE(model_name, to_hex(model_id), 'No Task') AS model, -- Model name from query_5169304, hex model_id, or 'No Task'
  CAST(CAST(task_fee AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS task_fee_aius, -- Task fee in decimal AIUS
  CAST(CAST(validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_rewards_aius, -- Validator rewards in decimal AIUS
  CAST(CAST(validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS validator_fees_aius, -- Validator fees in decimal AIUS
  CAST(CAST(profit_aius_tokens AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS profit_aius_tokens -- Net profit in decimal AIUS
FROM task_summary
ORDER BY task_block_number, evt_tx_index, evt_index -- Order by on-chain submission sequence



-- (Live) Tasks Submitted with Total Fees and Incentives
-- Retrieve TaskSubmitted events from the last 15 minutes with their fee and aggregate associated incentives
WITH task_submitted AS (
  SELECT
    evt_tx_hash AS tx_hash, -- Transaction hash for joining
    evt_index AS index, -- Event log index for reference
    evt_block_time AS block_time, -- Timestamp for filtering and sorting
    id AS task_id, -- Task ID for linking
    model AS model_id, -- Model hash (varbinary)
    fee / POWER(10, 18) AS task_fee -- Initial task fee, adjusted for 18 decimals
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  WHERE evt_block_time >= NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE -- Last x minutes from current time
),
-- Aggregate total incentives (amount) per task from IncentiveAdded events in the router contract
incentive_totals AS (
  SELECT
    taskid AS task_id, -- Task ID from taskid column (varbinary)
    SUM(amount) / POWER(10, 18) AS total_incentives -- Sum incentives and adjust for 18 decimals
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded
  GROUP BY taskid
),
-- Combine TaskSubmitted with fee, incentives, and model information
tasks_with_details AS (
  SELECT
    t.block_time,
    t.tx_hash,
    t.task_id,
    to_hex(t.model_id) AS model_id, -- Convert model_id to varchar
    COALESCE(m.model_name, to_hex(t.model_id), 'No Task') AS model_name, -- Get model name or fallback
    COALESCE(t.task_fee, 0) AS task_fee, -- Initial task fee from TaskSubmitted
    COALESCE(i.total_incentives, 0) AS total_incentives, -- Total incentives from IncentiveAdded
    COALESCE(t.task_fee, 0) + COALESCE(i.total_incentives, 0) AS total_fees_and_incentives -- Sum of task fee and incentives
  FROM task_submitted t
  LEFT JOIN incentive_totals i ON t.task_id = i.task_id
  LEFT JOIN query_5169304 m ON to_hex(t.model_id) = to_hex(m.model_id) -- Match model_id for model_name
)
-- Output results ordered by submission time
SELECT
  block_time,
  tx_hash,
  task_id,
  model_id,
  model_name,
  task_fee,
  total_incentives,
  total_fees_and_incentives
FROM tasks_with_details
ORDER BY block_time
