WITH time_series AS (
  SELECT time
  FROM (
    SELECT sequence(
      CAST(NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE AS timestamp(3)),
      CAST(NOW() AS timestamp(3)),
      INTERVAL '{{interval_minutes}}' MINUTE
    ) AS date_array
  ) ts
  CROSS JOIN UNNEST(date_array) AS t(time)
),
events AS (
  SELECT
    evt_block_time AS time,
    model,
    1 AS delta
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  UNION ALL
  SELECT
    s.evt_block_time AS time,
    t.model,
    -1 AS delta
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t ON s.task = t.id
),
running_sums AS (
  SELECT
    time,
    model,
    SUM(delta) OVER (PARTITION BY model ORDER BY time) AS unsolved_tasks
  FROM events
),
time_model AS (
  SELECT ts.time AS ts_time, m.model
  FROM time_series ts
  CROSS JOIN (SELECT DISTINCT model FROM events) m
),
ranked AS (
  SELECT
    tm.ts_time,
    tm.model,
    rs.unsolved_tasks,
    ROW_NUMBER() OVER (PARTITION BY tm.ts_time, tm.model ORDER BY rs.time DESC) AS rn
  FROM time_model tm
  LEFT JOIN running_sums rs ON rs.model = tm.model AND rs.time <= tm.ts_time
)
SELECT
  tm.ts_time AS time,
  COALESCE(m.model_name, to_hex(tm.model)) AS model,
  COALESCE(r.unsolved_tasks, 0) AS unsolved_tasks
FROM time_model tm
LEFT JOIN (
  SELECT ts_time, model, unsolved_tasks
  FROM ranked
  WHERE rn = 1
) r ON r.ts_time = tm.ts_time AND r.model = tm.model
LEFT JOIN query_5169304 m ON tm.model = m.model_id
ORDER BY tm.ts_time, tm.model
