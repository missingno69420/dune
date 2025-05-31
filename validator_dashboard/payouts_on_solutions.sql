-- https://dune.com/queries/5192613/
WITH params AS (
  SELECT
    CAST(NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE AS TIMESTAMP(3)) AS start_time,
    CAST(NOW() AS TIMESTAMP(3)) AS end_time,
    INTERVAL '{{interval_minutes}}' MINUTE AS interval
),
time_series AS (
  SELECT time
  FROM (
    SELECT sequence(
      p.start_time,
      p.end_time,
      p.interval
    ) AS date_array
    FROM params p
  ) ts
  CROSS JOIN UNNEST(date_array) AS t(time)
),
time_intervals AS (
  SELECT
    time AS interval_start,
    LEAD(time, 1, (SELECT end_time FROM params)) OVER (ORDER BY time) AS interval_end
  FROM time_series
),
solutions AS (
  SELECT
    task AS task_id,
    evt_block_time AS claim_time
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed
  WHERE evt_block_time >= (SELECT start_time FROM params)
    AND evt_block_time <= (SELECT end_time FROM params)
),
payouts AS (
  SELECT
    task_id,
    SUM(CASE WHEN event_type = 'RewardsPaid' THEN validator_reward ELSE 0 END) AS total_rewards,
    SUM(CASE WHEN event_type = 'FeesPaid' THEN validator_fee ELSE 0 END) AS total_fees
  FROM query_5179596
  GROUP BY task_id
),
incentives AS (
  SELECT
    taskid AS task_id,
    SUM(amount) AS total_incentives
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed
  GROUP BY taskid
),
total_payouts AS (
  SELECT
    s.task_id,
    s.claim_time,
    t.model AS model_id,
    COALESCE(p.total_rewards, 0) + COALESCE(p.total_fees, 0) + COALESCE(i.total_incentives, 0) AS total_payout
  FROM solutions s
  LEFT JOIN payouts p ON s.task_id = p.task_id
  LEFT JOIN incentives i ON s.task_id = i.task_id
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t ON s.task_id = t.id
),
payout_intervals AS (
  SELECT
    ti.interval_start,
    tp.model_id,
    tp.total_payout
  FROM total_payouts tp
  JOIN time_intervals ti
    ON tp.claim_time >= ti.interval_start
    AND tp.claim_time < ti.interval_end
),
min_max_per_interval AS (
  SELECT
    interval_start,
    model_id,
    MIN(total_payout) AS min_payout,
    MAX(total_payout) AS max_payout
  FROM payout_intervals
  GROUP BY interval_start, model_id
)
SELECT
  mmpi.interval_start AS datetime,
  COALESCE(m.model_name, TO_HEX(mmpi.model_id)) AS model,
  COALESCE(mmpi.min_payout / POWER(10, 18), 0) AS min_payout_aius,
  COALESCE(mmpi.max_payout / POWER(10, 18), 0) AS max_payout_aius
FROM min_max_per_interval mmpi
LEFT JOIN query_5169304 m ON mmpi.model_id = m.model_id
ORDER BY mmpi.interval_start, mmpi.model_id;
