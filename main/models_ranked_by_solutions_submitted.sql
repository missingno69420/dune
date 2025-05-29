-- https://dune.com/queries/5203680
WITH models AS (
  SELECT id AS model
  FROM arbius_arbitrum.v2_enginev5_1_evt_modelregistered
),
task_counts AS (
  SELECT model, COUNT(*) as task_count
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  GROUP BY model
),
solution_counts AS (
  SELECT t.model, COUNT(*) as solution_count
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
  JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t ON s.task = t.id
  GROUP BY t.model
)
SELECT
  CASE
    WHEN LENGTH(COALESCE(mn.model_name, to_hex(m.model))) > 8
    THEN SUBSTRING(COALESCE(mn.model_name, to_hex(m.model)) FROM 1 FOR 5) || '...'
    ELSE COALESCE(mn.model_name, to_hex(m.model))
  END AS model_name,
  COALESCE(t.task_count, 0) as task_count,
  COALESCE(s.solution_count, 0) as solution_count
FROM models m
LEFT JOIN task_counts t ON m.model = t.model
LEFT JOIN solution_counts s ON m.model = s.model
LEFT JOIN query_5169304 mn ON m.model = mn.model_id
ORDER BY solution_count DESC;
