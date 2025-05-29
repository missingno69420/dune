SELECT
  kind,
  block_time,
  task_id,
  tx_hash,
  evt_index,
  task_fee,
  model_owner_fee,
  treasury_fee,
  validator_fee,
  treasury_reward,
  task_owner_reward,
  validator_reward,
  incentive_paid
FROM (
  -- TaskSubmitted event
  SELECT
    'TaskSubmitted' AS kind,
    t.evt_block_time AS block_time,
    t.id AS task_id,
    t.evt_tx_hash AS tx_hash,
    t.evt_index AS evt_index,
    t.fee AS task_fee,
    NULL AS model_owner_fee,
    NULL AS treasury_fee,
    NULL AS validator_fee,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS incentive_paid
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
  WHERE t.id = {{task_id}}

  UNION ALL

  -- SolutionClaimed event from query_5179596
  SELECT
    'SolutionClaimed' AS kind,
    p.block_time,
    p.task_id,
    p.tx_hash,
    p.index AS evt_index,
    NULL AS task_fee,
    NULL AS model_owner_fee,
    NULL AS treasury_fee,
    NULL AS validator_fee,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS incentive_paid
  FROM query_5179596 p
  WHERE p.task_id = {{task_id}} AND p.event_type = 'SolutionClaimed'

  UNION ALL

  -- FeesPaid event from query_5179596
  SELECT
    'FeesPaid' AS kind,
    p.block_time,
    p.task_id,
    p.tx_hash,
    p.index AS evt_index,
    NULL AS task_fee,
    p.model_owner_fee,
    p.treasury_total_fee AS treasury_fee,
    p.validator_fee,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    NULL AS incentive_paid
  FROM query_5179596 p
  WHERE p.task_id = {{task_id}} AND p.event_type = 'FeesPaid'

  UNION ALL

  -- RewardsPaid event from query_5179596
  SELECT
    'RewardsPaid' AS kind,
    p.block_time,
    p.task_id,
    p.tx_hash,
    p.index AS evt_index,
    NULL AS task_fee,
    NULL AS model_owner_fee,
    NULL AS treasury_fee,
    NULL AS validator_fee,
    p.treasury_reward,
    p.task_owner_reward,
    p.validator_reward,
    NULL AS incentive_paid
  FROM query_5179596 p
  WHERE p.task_id = {{task_id}} AND p.event_type = 'RewardsPaid'

  UNION ALL

  -- IncentiveClaimed event
  SELECT
    'IncentiveClaimed' AS kind,
    i.evt_block_time AS block_time,
    i.taskid AS task_id,
    i.evt_tx_hash AS tx_hash,
    i.evt_index AS evt_index,
    NULL AS task_fee,
    NULL AS model_owner_fee,
    NULL AS treasury_fee,
    NULL AS validator_fee,
    NULL AS treasury_reward,
    NULL AS task_owner_reward,
    NULL AS validator_reward,
    i.amount AS incentive_paid
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed i
  WHERE i.taskid = {{task_id}}
) AS all_events
ORDER BY block_time, evt_index;
