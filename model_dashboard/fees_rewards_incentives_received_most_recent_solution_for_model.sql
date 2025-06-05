-- https://dune.com/queries/5190382/
WITH latest_claimed_solution AS (
  SELECT
    s.task AS task_id,
    s.evt_block_time AS solution_block_time,
    s.evt_tx_hash AS solution_tx,
    t.evt_tx_hash AS task_tx
  FROM arbius_arbitrum.engine_evt_solutionclaimed s
  JOIN arbius_arbitrum.engine_evt_tasksubmitted t ON s.task = t.id
  WHERE t.model = {{model_id}}
  ORDER BY s.evt_block_time DESC, s.evt_index DESC
  LIMIT 1
),
payouts AS (
  SELECT
    p.task_id,
    SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_reward,
    SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fee
  FROM query_5179596 p
  GROUP BY p.task_id
),
incentives AS (
  SELECT
    ia.taskid AS task_id,
    SUM(ia.amount) AS incentives_added
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded ia
  GROUP BY ia.taskid
)
SELECT
  ls.task_id,
  ls.solution_block_time,
  ls.task_tx,
  ls.solution_tx,
  COALESCE(p.validator_fee, 0) / POWER(10, 18) AS validator_fee_aius,
  COALESCE(p.validator_reward, 0) / POWER(10, 18) AS validator_reward_aius,
  COALESCE(i.incentives_added, 0) / POWER(10, 18) AS incentives_added_aius,
  (COALESCE(p.validator_fee, 0) + COALESCE(p.validator_reward, 0) + COALESCE(i.incentives_added, 0)) / POWER(10, 18) AS total_payout_aius
FROM latest_claimed_solution ls
LEFT JOIN payouts p ON ls.task_id = p.task_id
LEFT JOIN incentives i ON ls.task_id = i.task_id;
