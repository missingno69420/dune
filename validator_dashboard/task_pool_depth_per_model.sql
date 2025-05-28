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
initial_state AS (
  SELECT
    t.model,
    COUNT(*) AS initial_unsolved
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
  CROSS JOIN params p
  LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
    ON t.id = s.task AND s.evt_block_time < p.start_time
  WHERE t.evt_block_time < p.start_time AND s.task IS NULL
  GROUP BY t.model
),
events AS (
  -- Initial state as an event at start_time
  SELECT
    p.start_time AS time,
    i.model,
    i.initial_unsolved AS delta
  FROM initial_state i
  CROSS JOIN params p
  UNION ALL
  -- Tasks within lookback period
  SELECT
    evt_block_time AS time,
    model,
    1 AS delta
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  CROSS JOIN params p
  WHERE evt_block_time >= p.start_time AND evt_block_time <= p.end_time
  UNION ALL
  -- Solutions within lookback period
  SELECT
    s.evt_block_time AS time,
    t.model,
    -1 AS delta
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t ON s.task = t.id
  CROSS JOIN params p
  WHERE s.evt_block_time >= p.start_time AND s.evt_block_time <= p.end_time
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
  tm.ts_time AS datetime,
  format_datetime(tm.ts_time, 'HH:mm:ss') AS time,
  COALESCE(m.model_name, TO_HEX(tm.model)) AS model,
  COALESCE(r.unsolved_tasks, 0) AS unsolved_tasks
FROM time_model tm
LEFT JOIN (
  SELECT ts_time, model, unsolved_tasks
  FROM ranked
  WHERE rn = 1
) r ON r.ts_time = tm.ts_time AND r.model = tm.model
LEFT JOIN query_5169304 m ON tm.model = m.model_id
ORDER BY tm.ts_time, tm.model;
