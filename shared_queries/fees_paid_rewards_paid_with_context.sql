-- https://dune.com/queries/5179596
-- Combine all events (RewardsPaid, FeesPaid, SolutionClaimed, ContestationVoteFinish) into a single CTE
-- Include required columns, keeping token values in raw wei and calculating treasury_total_fee
WITH all_events AS (
  SELECT
    evt_tx_hash AS tx_hash, -- Transaction hash for grouping events
    evt_index AS index, -- Event log index for ordering within a transaction
    evt_block_time AS block_time, -- Timestamp for filtering or aggregation
    'RewardsPaid' AS event_type, -- Label for RewardsPaid events
    NULL AS task_id, -- Placeholder for task ID (to be assigned later)
    totalRewards AS total_rewards, -- Total rewards from RewardsPaid (raw wei)
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
    NULL AS total_rewards, -- No total rewards for FeesPaid
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
    NULL AS total_rewards, -- No total rewards for SolutionClaimed
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
    NULL AS total_rewards, -- No total rewards for ContestationVoteFinish
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
    total_rewards, -- Carry through total rewards
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
    r.total_rewards, -- Include total rewards in output
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
  total_rewards, -- vote escrow receives total_rewards
  treasury_reward,
  task_owner_reward,
  validator_reward,
  model_owner_fee,
  treasury_total_fee,
  validator_fee,
  task_id
FROM events_with_task_id
ORDER BY block_time, tx_hash, index
