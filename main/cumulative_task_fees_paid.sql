-- https://dune.com/queries/5165926/8505115
WITH first_date AS (
  SELECT MIN(DATE(evt_block_time)) AS first_day
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
),
date_series AS (
  SELECT time AS day
  FROM (
    SELECT sequence(
      (SELECT first_day FROM first_date),
      CURRENT_DATE,
      INTERVAL '1' DAY
    ) AS date_array
  ) AS s
  CROSS JOIN UNNEST(date_array) AS t(time)
),
unique_models AS (
  SELECT DISTINCT model AS model_id
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
),
grid AS (
  SELECT d.day, m.model_id
  FROM date_series d
  CROSS JOIN unique_models m
),
daily_fees AS (
  SELECT
    DATE(evt_block_time) AS day,
    model AS model_id,
    SUM(fee / 1e18) AS daily_task_fees
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  GROUP BY 1, 2
),
grid_with_fees AS (
  SELECT
    g.day,
    g.model_id,
    COALESCE(df.daily_task_fees, 0) AS daily_task_fees
  FROM grid g
  LEFT JOIN daily_fees df ON g.day = df.day AND g.model_id = df.model_id
),
cumulative_fees AS (
  SELECT
    day,
    model_id,
    SUM(daily_task_fees) OVER (PARTITION BY model_id ORDER BY day) AS cumulative_task_fees
  FROM grid_with_fees
)
SELECT
  c.day,
  COALESCE(m.model_name, TO_HEX(c.model_id)) AS model,
  c.cumulative_task_fees
FROM cumulative_fees c
LEFT JOIN query_5169304 m ON c.model_id = m.model_id
ORDER BY c.day, COALESCE(m.model_name, TO_HEX(c.model_id)) DESC;
