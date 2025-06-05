-- https://dune.com/queries/5192366/
WITH relevant_events AS (
  -- Select SolutionClaimed events
  SELECT
    'SolutionClaimed' AS event_type,
    s.evt_block_time AS block_time,
    s.evt_tx_hash AS event_tx,
    s.evt_tx_index AS tx_index,
    s.evt_index AS log_index,
    s.task AS task_id,
    t.evt_tx_hash AS task_tx
  FROM arbius_arbitrum.engine_evt_solutionclaimed s
  JOIN arbius_arbitrum.engine_evt_tasksubmitted t ON s.task = t.id
  WHERE t.model = {{model_id}}
  UNION ALL
  -- Select ContestationVoteFinish events
  SELECT
    'ContestationVoteFinish' AS event_type,
    c.evt_block_time AS block_time,
    c.evt_tx_hash AS event_tx,
    c.evt_tx_index AS tx_index,
    c.evt_index AS log_index,
    c.id AS task_id,
    t.evt_tx_hash AS task_tx
  FROM arbius_arbitrum.engine_evt_contestationvotefinish c
  JOIN arbius_arbitrum.engine_evt_tasksubmitted t ON c.id = t.id
  WHERE t.model = {{model_id}}
),
last_n_events AS (
  -- Select the most recent N events with rank
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY block_time DESC, tx_index DESC, log_index DESC) AS rank
  FROM relevant_events
  ORDER BY block_time DESC, tx_index DESC, log_index DESC
  LIMIT {{recent_payouts_length}}
),
relevant_tasks AS (
  -- Get distinct task_ids from the last N events
  SELECT DISTINCT task_id
  FROM last_n_events
),
payouts AS (
  -- Aggregate validator rewards and fees for relevant tasks only
  SELECT
    p.task_id,
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_reward,
    SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fee
  FROM query_5179596 p
  WHERE p.task_id IN (SELECT task_id FROM relevant_tasks)
  GROUP BY p.task_id
),
incentives_cumulative AS (
  -- Compute cumulative incentives for relevant tasks
  SELECT
    ia.taskid,
    ia.evt_block_time,
    ia.evt_tx_index,
    ia.evt_index,
    SUM(ia.amount) OVER (
      PARTITION BY ia.taskid
      ORDER BY ia.evt_block_time, ia.evt_tx_index, ia.evt_index
    ) AS cumulative_amount
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded ia
  WHERE ia.taskid IN (SELECT task_id FROM relevant_tasks)
),
event_incentives AS (
  -- Compute the maximum cumulative incentive amount for each event
  SELECT
    e.rank,
    COALESCE(MAX(ic.cumulative_amount), 0) / POWER(10, 18) AS incentives_added_aius
  FROM last_n_events e
  LEFT JOIN incentives_cumulative ic ON ic.taskid = e.task_id AND ic.evt_block_time <= e.block_time
  GROUP BY e.rank
)
SELECT
  e.rank,
  e.event_type,
  e.task_id,
  e.block_time AS event_block_time,
  e.task_tx,
  e.event_tx,
  COALESCE(p.validator_fee, 0) / POWER(10, 18) AS validator_fee_aius,
  COALESCE(p.validator_reward, 0) / POWER(10, 18) AS validator_reward_aius,
  ei.incentives_added_aius,
  (
    COALESCE(p.validator_fee, 0) / POWER(10, 18) +
    COALESCE(p.validator_reward, 0) / POWER(10, 18) +
    ei.incentives_added_aius
  ) AS total_payout_aius
FROM last_n_events e
LEFT JOIN payouts p ON e.task_id = p.task_id
LEFT JOIN event_incentives ei ON e.rank = ei.rank
ORDER BY e.rank ASC;
