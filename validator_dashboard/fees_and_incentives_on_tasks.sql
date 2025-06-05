-- https://dune.com/queries/5192052/
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
task_submitted AS (
  SELECT
    t.evt_tx_hash AS tx_hash,
    t.evt_block_time AS block_time,
    t.id AS task_id,
    t.model AS model_id,
    t.fee AS task_fee,
    COALESCE(i.amount, 0) AS initial_incentive
  FROM arbius_arbitrum.engine_evt_tasksubmitted t
  LEFT JOIN arbius_arbitrum.arbiusrouterv1_evt_incentiveadded i
    ON t.evt_tx_hash = i.evt_tx_hash AND t.id = i.taskid
  WHERE t.evt_block_time >= (SELECT start_time FROM params)
    AND t.evt_block_time <= (SELECT end_time FROM params)
),
tasks_with_totals AS (
  SELECT
    task_id,
    block_time,
    model_id,
    (task_fee + initial_incentive) AS initial_total
  FROM task_submitted
),
task_intervals AS (
  SELECT
    ti.interval_start,
    twt.model_id,
    twt.initial_total
  FROM tasks_with_totals twt
  JOIN time_intervals ti
    ON twt.block_time >= ti.interval_start
    AND twt.block_time < ti.interval_end
),
min_max_per_interval AS (
  SELECT
    interval_start,
    model_id,
    MIN(initial_total) AS min_initial_total,
    MAX(initial_total) AS max_initial_total
  FROM task_intervals
  GROUP BY interval_start, model_id
)
SELECT
  mmpi.interval_start AS datetime,
  COALESCE(m.model_name, TO_HEX(mmpi.model_id)) AS model,
  COALESCE(mmpi.min_initial_total / POWER(10, 18), 0) AS min_initial_total_aius,
  COALESCE(mmpi.max_initial_total / POWER(10, 18), 0) AS max_initial_total_aius
FROM min_max_per_interval mmpi
LEFT JOIN query_5169304 m ON mmpi.model_id = m.model_id
ORDER BY mmpi.interval_start, mmpi.model_id;
