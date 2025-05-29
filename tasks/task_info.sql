WITH all_events AS (
  -- TaskSubmitted: When the task was submitted
  SELECT
    'TaskSubmitted' AS kind,
    t.evt_block_time AS block_time,
    t.id AS task_id,
    t.evt_tx_hash AS tx_hash,
    t.evt_index AS evt_index,
    'fee: ' || CAST(t.fee AS VARCHAR) AS details
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
  WHERE t.id = {{task_id}}

  UNION ALL

  -- SolutionSubmitted: When a solution was submitted for the task
  SELECT
    'SolutionSubmitted' AS kind,
    s.evt_block_time AS block_time,
    s.task AS task_id,
    s.evt_tx_hash AS tx_hash,
    s.evt_index AS evt_index,
    'submitted by: ' || CAST(s.addr AS VARCHAR) AS details
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
  WHERE s.task = {{task_id}}

  UNION ALL

  -- SolutionClaimed: When a solution was claimed
  SELECT
    'SolutionClaimed' AS kind,
    s.evt_block_time AS block_time,
    s.task AS task_id,
    s.evt_tx_hash AS tx_hash,
    s.evt_index AS evt_index,
    'claimed by: ' || CAST(s.addr AS VARCHAR) AS details
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed s
  WHERE s.task = {{task_id}}

  UNION ALL

  -- ContestationVote: When a validator voted on a contestation
  SELECT
    'ContestationVote' AS kind,
    v.evt_block_time AS block_time,
    v.task AS task_id,
    v.evt_tx_hash AS tx_hash,
    v.evt_index AS evt_index,
    'validator: ' || CAST(v.addr AS VARCHAR) || ', yea: ' || CAST(v.yea AS VARCHAR) AS details
  FROM arbius_arbitrum.v2_enginev5_1_evt_contestationvote v
  WHERE v.task = {{task_id}}

  UNION ALL

  -- ContestationVoteFinish: When the contestation voting concluded
  SELECT
    'ContestationVoteFinish' AS kind,
    f.evt_block_time AS block_time,
    f.id AS task_id,
    f.evt_tx_hash AS tx_hash,
    f.evt_index AS evt_index,
    'start_idx: ' || CAST(f.start_idx AS VARCHAR) || ', end_idx: ' || CAST(f.end_idx AS VARCHAR) AS details
  FROM arbius_arbitrum.v2_enginev5_1_evt_contestationvotefinish f
  WHERE f.id = {{task_id}}

  UNION ALL

  -- IncentiveAdded: When an incentive was added to the task
  SELECT
    'IncentiveAdded' AS kind,
    a.evt_block_time AS block_time,
    a.taskid AS task_id,
    a.evt_tx_hash AS tx_hash,
    a.evt_index AS evt_index,
    'amount: ' || CAST(a.amount AS VARCHAR) AS details
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded a
  WHERE a.taskid = {{task_id}}

  UNION ALL

  -- FeesPaid: Fees distributed for the task (using raw values)
  SELECT
    'FeesPaid' AS kind,
    p.block_time,
    p.task_id,
    p.tx_hash,
    p.index AS evt_index,
    'model_fee: ' || CAST(p.model_fee AS VARCHAR) ||
    ', treasury_fee: ' || CAST(p.treasury_fee AS VARCHAR) ||
    ', remaining_fee: ' || CAST(p.remaining_fee AS VARCHAR) ||
    ', validator_fee: ' || CAST(p.validator_fee AS VARCHAR) AS details
  FROM query_5206621 p
  WHERE p.task_id = {{task_id}} AND p.event_type = 'FeesPaid'

  UNION ALL

  -- RewardsPaid: Rewards distributed for the task (using raw values)
  SELECT
    'RewardsPaid' AS kind,
    p.block_time,
    p.task_id,
    p.tx_hash,
    p.index AS evt_index,
    'total_rewards: ' || CAST(p.total_rewards AS VARCHAR) ||
    ', treasury_reward: ' || CAST(p.treasury_reward AS VARCHAR) ||
    ', task_owner_reward: ' || CAST(p.task_owner_reward AS VARCHAR) ||
    ', validator_reward: ' || CAST(p.validator_reward AS VARCHAR) AS details
  FROM query_5206621 p
  WHERE p.task_id = {{task_id}} AND p.event_type = 'RewardsPaid'

  UNION ALL

  -- IncentiveClaimed: When an incentive was claimed
  SELECT
    'IncentiveClaimed' AS kind,
    i.evt_block_time AS block_time,
    i.taskid AS task_id,
    i.evt_tx_hash AS tx_hash,
    i.evt_index AS evt_index,
    'amount: ' || CAST(i.amount AS VARCHAR) AS details
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed i
  WHERE i.taskid = {{task_id}}
)
SELECT
  kind,
  block_time,
  task_id,
  tx_hash,
  evt_index,
  details
FROM all_events
ORDER BY block_time, evt_index;
