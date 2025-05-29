-- https://dune.com/queries/5206621
WITH all_events AS (
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'RewardsPaid' AS event_type,
    NULL AS task_id,
    totalRewards AS total_rewards,
    treasuryReward AS treasury_reward,
    taskOwnerReward AS task_owner_reward,
    validatorReward AS validator_reward,
    NULL AS model_fee,
    NULL AS treasury_fee,
    NULL AS remaining_fee,
    NULL AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_rewardspaid
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'FeesPaid' AS event_type,
    NULL AS task_id,
    NULL AS total_rewards,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    modelFee AS model_fee,
    treasuryFee AS treasury_fee,
    remainingFee AS remaining_fee,
    validatorFee AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'SolutionClaimed' AS event_type,
    task AS task_id,
    NULL AS total_rewards,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS model_fee,
    NULL AS treasury_fee,
    NULL AS remaining_fee,
    NULL AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'ContestationVoteFinish' AS event_type,
    id AS task_id,
    NULL AS total_rewards,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS model_fee,
    NULL AS treasury_fee,
    NULL AS remaining_fee,
    NULL AS validator_fee
  FROM arbius_arbitrum.v2_enginev5_1_evt_contestationvotefinish
),
events_with_context AS (
  SELECT
    *,
    LEAD(event_type) OVER (PARTITION BY tx_hash ORDER BY index) AS next_event_type,
    LEAD(event_type, 2) OVER (PARTITION BY tx_hash ORDER BY index) AS next_next_event_type,
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END) OVER (PARTITION BY tx_hash ORDER BY index) AS next_contestation_task_id,
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END, 2) OVER (PARTITION BY tx_hash ORDER BY index) AS next_next_contestation_task_id
  FROM all_events
),
rewards_fees_events AS (
  SELECT
    tx_hash,
    index,
    block_time,
    event_type,
    total_rewards,
    treasury_reward,
    task_owner_reward,
    validator_reward,
    model_fee,
    treasury_fee,
    remaining_fee,
    validator_fee,
    next_event_type,
    next_next_event_type,
    next_contestation_task_id,
    next_next_contestation_task_id
  FROM events_with_context
  WHERE event_type IN ('RewardsPaid', 'FeesPaid')
),
fees_candidates AS (
  SELECT
    r.tx_hash,
    r.index AS rewards_fees_index,
    r.event_type,
    f.evt_index AS fees_index,
    ROW_NUMBER() OVER (PARTITION BY r.tx_hash, r.index ORDER BY f.evt_index DESC) AS fee_rank
  FROM rewards_fees_events r
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_feespaid f
    ON r.tx_hash = f.evt_tx_hash
    AND f.evt_index < r.index
  WHERE r.event_type = 'RewardsPaid'
    OR r.event_type = 'FeesPaid'
),
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
events_with_task_id AS (
  SELECT
    r.block_time,
    r.tx_hash,
    r.index,
    r.event_type,
    r.total_rewards,
    r.treasury_reward,
    r.task_owner_reward,
    r.validator_reward,
    r.model_fee,
    r.treasury_fee,
    r.remaining_fee,
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
      WHEN r.event_type IN ('SolutionClaimed', 'ContestationVoteFinish') THEN r.task_id
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
SELECT
  block_time,
  tx_hash,
  index,
  event_type,
  total_rewards,
  treasury_reward,
  task_owner_reward,
  validator_reward,
  model_fee,
  treasury_fee,
  remaining_fee,
  validator_fee,
  task_id
FROM events_with_task_id
ORDER BY block_time, tx_hash, index
