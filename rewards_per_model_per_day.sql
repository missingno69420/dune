-- Combine all relevant events (RewardsPaid, FeesPaid, SolutionClaimed, ContestationVoteFinish) into a single CTE
-- Each event type includes fields needed for aggregation (rewards, fees) or task ID assignment
WITH all_events AS (
  SELECT
    evt_tx_hash AS tx_hash, -- Transaction hash for grouping events
    evt_index AS index, -- Event log index for ordering within a transaction
    evt_block_time AS block_time, -- Timestamp for daily aggregation
    'RewardsPaid' AS event_type, -- Label for RewardsPaid events
    NULL AS task_id, -- Placeholder for task ID (to be assigned later)
    treasuryReward AS treasury_reward, -- Reward to treasury (raw wei)
    taskOwnerReward AS task_owner_reward, -- Reward to task owner (raw wei)
    validatorReward AS validator_reward, -- Reward to validator (raw wei)
    NULL AS model_owner_fee, -- Placeholder for FeesPaid fields
    NULL AS treasury_total_fee,
    NULL AS validator_fee
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
    validatorFee AS validator_fee -- Fee to validator (raw wei)
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'SolutionClaimed' AS event_type,
    task AS task_id, -- Task ID for linking to models
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
    id AS task_id, -- Task ID for linking to models
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS model_owner_fee,
    NULL AS treasury_total_fee,
    NULL AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_contestationvotefinish
),
-- Add context for task ID assignment by capturing next events and task IDs
-- Only LEAD is needed since we look forward for ContestationVoteFinish
events_with_context AS (
  SELECT
    *,
    LEAD(event_type) OVER (PARTITION BY tx_hash ORDER BY index) AS next_event_type, -- Next event in the transaction
    LEAD(event_type, 2) OVER (PARTITION BY tx_hash ORDER BY index) AS next_next_event_type, -- Event after the next (for FeesPaid logic)
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END) OVER (PARTITION BY tx_hash ORDER BY index) AS next_contestation_task_id, -- Task ID of next ContestationVoteFinish
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END, 2) OVER (PARTITION BY tx_hash ORDER BY index) AS next_next_contestation_task_id -- Task ID of ContestationVoteFinish after next
  FROM all_events
),
-- Filter to RewardsPaid and FeesPaid events, which have rewards/fees to aggregate
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
-- Find the closest preceding FeesPaid event for RewardsPaid and some FeesPaid events
-- For FeesPaid, include all to ensure task ID assignment
fees_candidates AS (
  SELECT
    r.tx_hash,
    r.index AS rewards_fees_index, -- Index of the RewardsPaid or FeesPaid event
    r.event_type,
    f.evt_index AS fees_index, -- Index of the FeesPaid event
    ROW_NUMBER() OVER (PARTITION BY r.tx_hash, r.index ORDER BY f.evt_index DESC) AS fee_rank -- Rank FeesPaid events by proximity (1 = closest)
  FROM rewards_fees_events r
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_feespaid f
    ON r.tx_hash = f.evt_tx_hash
    AND f.evt_index < r.index -- FeesPaid must precede the event
  WHERE r.event_type = 'RewardsPaid'
    OR r.event_type = 'FeesPaid'
),
-- Find the closest preceding SolutionClaimed event for each matched FeesPaid
solution_candidates AS (
  SELECT
    f.tx_hash,
    f.rewards_fees_index,
    f.event_type,
    f.fees_index,
    s.evt_index AS solution_index, -- Index of the SolutionClaimed event
    s.task AS solution_task_id, -- Task ID from SolutionClaimed
    ROW_NUMBER() OVER (PARTITION BY f.tx_hash, f.rewards_fees_index, f.fees_index ORDER BY s.evt_index DESC) AS solution_rank -- Rank SolutionClaimed by proximity
  FROM fees_candidates f
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed s
    ON f.tx_hash = s.evt_tx_hash
    AND s.evt_index < f.fees_index -- SolutionClaimed must precede FeesPaid
  WHERE f.fee_rank = 1 -- Only use the closest FeesPaid
),
-- Find the closest preceding SolutionClaimed event for FeesPaid events directly
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
-- Assign task IDs to RewardsPaid and FeesPaid events
rewards_fees_with_task_id AS (
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
    r.next_event_type,
    r.next_next_event_type,
    r.next_contestation_task_id,
    r.next_next_contestation_task_id,
    CASE
      WHEN r.event_type = 'FeesPaid' THEN
        CASE
          WHEN sf.solution_task_id IS NOT NULL THEN sf.solution_task_id -- Use closest preceding SolutionClaimed
          WHEN r.next_event_type = 'ContestationVoteFinish' THEN r.next_contestation_task_id -- Use next ContestationVoteFinish
          WHEN r.next_event_type = 'RewardsPaid' AND r.next_next_event_type = 'ContestationVoteFinish' THEN r.next_next_contestation_task_id -- Use ContestationVoteFinish after RewardsPaid
          ELSE NULL
        END
      WHEN r.event_type = 'RewardsPaid' THEN
        CASE
          WHEN s.solution_task_id IS NOT NULL THEN s.solution_task_id -- Use SolutionClaimed via FeesPaid
          WHEN r.next_event_type = 'ContestationVoteFinish' THEN r.next_contestation_task_id -- Use next ContestationVoteFinish
          ELSE NULL
        END
      ELSE NULL
    END AS task_id
  FROM rewards_fees_events r
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
),
-- Truncate block_time to day for aggregation and select fields for next steps
rewards_fees_with_tasks AS (
  SELECT
    date_trunc('day', block_time) AS day,
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
  FROM rewards_fees_with_task_id
  WHERE event_type IN ('RewardsPaid', 'FeesPaid')
),
-- Get model_id from TaskSubmitted for linking to model names
task_models AS (
  SELECT
    id AS task_id,
    model AS model_id -- Model hash (varbinary)
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
),
-- Convert model_id to varchar with to_hex and adjust token values for 18 decimals
rewards_fees_with_models AS (
  SELECT
    r.day,
    to_hex(m.model_id) AS model_id, -- Convert varbinary to varchar
    r.treasury_reward / POWER(10, 18) AS treasury_reward, -- Convert from wei to AIUS tokens
    r.task_owner_reward / POWER(10, 18) AS task_owner_reward,
    r.validator_reward / POWER(10, 18) AS validator_reward,
    r.model_owner_fee / POWER(10, 18) AS model_owner_fee,
    r.treasury_total_fee / POWER(10, 18) AS treasury_total_fee,
    r.validator_fee / POWER(10, 18) AS validator_fee
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
