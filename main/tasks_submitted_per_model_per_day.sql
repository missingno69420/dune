-- https://dune.com/queries/5165823/
WITH tasks_submitted AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    model AS model_id,
    COUNT(*) AS tasks_submitted
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  GROUP BY 1, 2
)
SELECT
  t.day,
  COALESCE(m.model_name, to_hex(t.model_id)) AS model,
  t.tasks_submitted
FROM tasks_submitted t
LEFT JOIN query_5169304 m ON t.model_id = m.model_id
ORDER BY t.day, COALESCE(m.model_name, to_hex(t.model_id))
