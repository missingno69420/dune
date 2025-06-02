WITH solutions_submitted AS (
  SELECT
    date_trunc('day', s.evt_block_time) AS day,
    t.model AS model_id,
    COUNT(*) AS solutions_submitted
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted s
  INNER JOIN arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
    ON s.task = t.id  -- Link solution to task for model info
  GROUP BY 1, t.model
)
SELECT
  s.day,
  COALESCE(m.model_name, to_hex(s.model_id)) AS model,
  s.solutions_submitted
FROM solutions_submitted s
LEFT JOIN query_5169304 m ON s.model_id = m.model_id
ORDER BY s.day, COALESCE(m.model_name, to_hex(s.model_id));
