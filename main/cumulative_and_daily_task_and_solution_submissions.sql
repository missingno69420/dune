WITH tasks_submitted AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    COUNT(*) AS tasks_submitted
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  GROUP BY 1
),
solutions_submitted AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    COUNT(*) AS solutions_submitted
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted
  GROUP BY 1
),
daily_counts AS (
  SELECT
    COALESCE(t.day, s.day) AS day,
    COALESCE(t.tasks_submitted, 0) AS tasks_submitted,
    COALESCE(s.solutions_submitted, 0) AS solutions_submitted
  FROM tasks_submitted t
  FULL OUTER JOIN solutions_submitted s ON t.day = s.day
)
SELECT
  day,
  tasks_submitted AS "Daily Tasks",
  solutions_submitted AS "Daily Solutions",
  SUM(tasks_submitted) OVER (ORDER BY day) AS "Cumulative Tasks",
  SUM(solutions_submitted) OVER (ORDER BY day) AS "Cumulative Solutions"
FROM daily_counts
ORDER BY day DESC;
