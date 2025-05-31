-- https://dune.com/queries/5192613/8544279
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
task_totals_per_time AS (
  SELECT
    ts.time AS T,
    t.model AS model_id,
    t.id AS task_id,
    t.fee / POWER(10, 18) + COALESCE(SUM(i.amount) / POWER(10, 18), 0) AS total
  FROM time_series ts
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
    ON t.evt_block_time <= ts.time
    AND t.evt_block_time >= (SELECT start_time FROM params)
  LEFT JOIN arbius_arbitrum.arbiusrouterv1_evt_incentiveadded i
    ON i.taskid = t.id
    AND i.evt_block_time >= t.evt_block_time
    AND i.evt_block_time <= ts.time
  GROUP BY ts.time, t.model, t.id, t.fee
),
rankings AS (
  SELECT
    T,
    model_id,
    task_id,
    total,
    ROW_NUMBER() OVER (PARTITION BY T, model_id ORDER BY total DESC) AS rank_max,
    ROW_NUMBER() OVER (PARTITION BY T, model_id ORDER BY total ASC) AS rank_min
  FROM task_totals_per_time
),
results AS (
  SELECT
    T,
    model_id,
    MAX(CASE WHEN rank_max = 1 THEN task_id END) AS max_task_id,
    MAX(CASE WHEN rank_max = 1 THEN total END) AS max_total,
    MAX(CASE WHEN rank_min = 1 THEN task_id END) AS min_task_id,
    MAX(CASE WHEN rank_min = 1 THEN total END) AS min_total
  FROM rankings
  GROUP BY T, model_id
)
SELECT
  r.T AS datetime,
  FORMAT_DATETIME(r.T, 'HH:mm:ss') AS time,
  COALESCE(m.model_name, TO_HEX(r.model_id)) AS model,
  r.max_task_id,
  r.max_total,
  r.min_task_id,
  r.min_total
FROM results r
LEFT JOIN query_5169304 m ON r.model_id = m.model_id
ORDER BY r.T, r.model_id;
