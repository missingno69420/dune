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
tasks AS (
  SELECT
    id AS task_id,
    model as model_id,
    evt_block_time AS submission_time
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
),
solutions AS (
  SELECT
    task AS task_id,
    evt_block_time AS submission_time
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted
)
SELECT
  ts.time,
  COUNT(DISTINCT t.task_id) AS unsolved_tasks,
  COALESCE(m.model_name, to_hex(t.model_id)) AS model
FROM time_series ts
LEFT JOIN tasks t
  ON t.submission_time < ts.time
LEFT JOIN solutions s
  ON s.task_id = t.task_id
  AND s.submission_time < ts.time
LEFT JOIN query_5169304 m ON t.model_id = m.model_id
WHERE s.task_id IS NULL
GROUP BY ts.time, COALESCE(m.model_name, to_hex(t.model_id))
ORDER BY ts.time
