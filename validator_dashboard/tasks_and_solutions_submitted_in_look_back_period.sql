-- https://dune.com/queries/5191924/8543318
WITH params AS (
  SELECT
    CAST(NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE AS TIMESTAMP) AS start_time,
    CAST(NOW() AS TIMESTAMP) AS end_time,
    INTERVAL '{{interval_minutes}}' MINUTE AS time_step
),
time_series AS (
  SELECT time
  FROM UNNEST(sequence(
    (SELECT start_time FROM params),
    (SELECT end_time FROM params),
    (SELECT time_step FROM params)
  )) AS t(time)
),
events AS (
  SELECT evt_block_time AS time, 1 AS task_count, 0 AS solution_count
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  WHERE evt_block_time >= (SELECT start_time FROM params)
    AND evt_block_time <= (SELECT end_time FROM params)
  UNION ALL
  SELECT evt_block_time AS time, 0 AS task_count, 1 AS solution_count
  FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted
  WHERE evt_block_time >= (SELECT start_time FROM params)
    AND evt_block_time <= (SELECT end_time FROM params)
),
running_sums AS (
  SELECT
    time,
    SUM(task_count) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_tasks,
    SUM(solution_count) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_solutions
  FROM events
),
joined AS (
  SELECT
    ts.time AS time_step,
    rs.time AS event_time,
    rs.cumulative_tasks,
    rs.cumulative_solutions,
    ROW_NUMBER() OVER (PARTITION BY ts.time ORDER BY rs.time DESC) AS rn
  FROM time_series ts
  LEFT JOIN running_sums rs ON rs.time <= ts.time
),
cumulative AS (
  SELECT
    time_step,
    COALESCE(cumulative_tasks, 0) AS cumulative_tasks,
    COALESCE(cumulative_solutions, 0) AS cumulative_solutions
  FROM joined
  WHERE rn = 1
),
incremental AS (
  SELECT
    time_step,
    cumulative_tasks,
    cumulative_solutions,
    cumulative_tasks - COALESCE(LAG(cumulative_tasks) OVER (ORDER BY time_step), 0) AS tasks_in_interval,
    cumulative_solutions - COALESCE(LAG(cumulative_solutions) OVER (ORDER BY time_step), 0) AS solutions_in_interval
  FROM cumulative
)
SELECT
  time_step,
  cumulative_tasks,
  cumulative_solutions,
  tasks_in_interval,
  solutions_in_interval
FROM incremental
ORDER BY time_step
