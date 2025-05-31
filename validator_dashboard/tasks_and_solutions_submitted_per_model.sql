-- https://dune.com/queries/5195013/8547890
WITH params AS (
  SELECT
    CAST(NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE AS TIMESTAMP) AS start_time,
    CAST(NOW() AS TIMESTAMP) AS end_time,
    INTERVAL '{{interval_minutes}}' MINUTE AS interval
),
time_series AS (
  SELECT time AS interval_start
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
models AS (
  SELECT DISTINCT model AS model_id
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  WHERE evt_block_time >= (SELECT start_time FROM params)
    AND evt_block_time <= (SELECT end_time FROM params)
  UNION
  SELECT DISTINCT t.model AS model_id
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t ON s.task = t.id
  WHERE s.evt_block_time >= (SELECT start_time FROM params)
    AND s.evt_block_time <= (SELECT end_time FROM params)
),
time_model_grid AS (
  SELECT ts.interval_start, m.model_id
  FROM time_series ts
  CROSS JOIN models m
),
tasks AS (
  SELECT
    ts.interval_start,
    t.model AS model_id,
    COUNT(*) AS tasks_submitted
  FROM time_series ts
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
    ON t.evt_block_time >= ts.interval_start
    AND t.evt_block_time < ts.interval_start + (SELECT interval FROM params)
  WHERE t.evt_block_time >= (SELECT start_time FROM params)
  GROUP BY ts.interval_start, t.model
),
solutions AS (
  SELECT
    ts.interval_start,
    t.model AS model_id,
    COUNT(*) AS solutions_submitted
  FROM time_series ts
  JOIN arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
    ON s.evt_block_time >= ts.interval_start
    AND s.evt_block_time < ts.interval_start + (SELECT interval FROM params)
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
    ON s.task = t.id
  WHERE s.evt_block_time >= (SELECT start_time FROM params)
  GROUP BY ts.interval_start, t.model
)
SELECT
  g.interval_start,
  COALESCE(m.model_name, to_hex(g.model_id)) AS model,
  COALESCE(t.tasks_submitted, 0) AS tasks_submitted,
  COALESCE(s.solutions_submitted, 0) AS solutions_submitted
FROM time_model_grid g
LEFT JOIN tasks t ON g.interval_start = t.interval_start AND g.model_id = t.model_id
LEFT JOIN solutions s ON g.interval_start = s.interval_start AND g.model_id = s.model_id
LEFT JOIN query_5169304 m ON g.model_id = m.model_id
ORDER BY g.interval_start, COALESCE(m.model_name, to_hex(g.model_id))
