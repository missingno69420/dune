-- https://dune.com/queries/5203680
WITH models AS (
  SELECT id AS model
  FROM arbius_arbitrum.engine_evt_modelregistered
),
solution_counts AS (
  SELECT t.model, COUNT(*) as solution_count
  FROM arbius_arbitrum.engine_evt_solutionsubmitted s
  JOIN arbius_arbitrum.engine_evt_tasksubmitted t ON s.task = t.id
  GROUP BY t.model
)
SELECT
  ROW_NUMBER() OVER (ORDER BY solution_count DESC) AS rank,
  COALESCE(mn.model_name, to_hex(m.model)) AS model_name,
  COALESCE(s.solution_count, 0) as solution_count
FROM models m
LEFT JOIN solution_counts s ON m.model = s.model
LEFT JOIN query_5169304 mn ON m.model = mn.model_id
ORDER BY solution_count DESC;
