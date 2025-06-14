-- https://dune.com/queries/5218733/
WITH min_max_days AS (
  SELECT
    MIN(CAST(DATE_TRUNC('day', block_time) AS DATE)) AS min_day,
    MAX(CAST(DATE_TRUNC('day', block_time) AS DATE)) AS max_day
  FROM query_5179596
),
date_series AS (
  SELECT
    time AS day
  FROM (
    SELECT
      sequence(
        (SELECT min_day FROM min_max_days),
        (SELECT max_day FROM min_max_days),
        INTERVAL '1' DAY
      ) AS date_array
  ) ts
  CROSS JOIN UNNEST(date_array) AS t(time)
),
unique_models AS (
  SELECT DISTINCT t.model_id
  FROM query_5179596 e
  LEFT JOIN (
    SELECT id AS task_id, model AS model_id
    FROM arbius_arbitrum.engine_evt_tasksubmitted
  ) t ON e.task_id = t.task_id
),
all_days_models AS (
  SELECT
    ds.day,
    um.model_id
  FROM date_series ds
  CROSS JOIN unique_models um
),
daily_sums AS (
  SELECT
    CAST(DATE_TRUNC('day', e.block_time) AS DATE) AS day,
    t.model_id,
    SUM(COALESCE(e.treasury_reward, 0)) / 1e18 AS daily_rewards,
    SUM(COALESCE(e.treasury_total_fee, 0)) / 1e18 AS daily_fees
  FROM query_5179596 e
  LEFT JOIN (
    SELECT id AS task_id, model AS model_id
    FROM arbius_arbitrum.engine_evt_tasksubmitted
  ) t ON e.task_id = t.task_id
  GROUP BY 1, 2
),
filled_data AS (
  SELECT
    adm.day,
    adm.model_id,
    COALESCE(ds.daily_rewards, 0) AS daily_rewards,
    COALESCE(ds.daily_fees, 0) AS daily_fees
  FROM all_days_models adm
  LEFT JOIN daily_sums ds ON adm.day = ds.day AND adm.model_id = ds.model_id
),
cumulative_data AS (
  SELECT
    day,
    model_id,
    SUM(daily_rewards) OVER (PARTITION BY model_id ORDER BY day) AS cumulative_rewards_aius,
    SUM(daily_fees) OVER (PARTITION BY model_id ORDER BY day) AS cumulative_fees_aius
  FROM filled_data
),
final_data AS (
  SELECT
    c.day,
    COALESCE(m.model_name, TO_HEX(c.model_id)) AS model,
    c.cumulative_rewards_aius,
    c.cumulative_fees_aius
  FROM cumulative_data c
  LEFT JOIN query_5169304 m ON c.model_id = m.model_id
)
SELECT
  day,
  model,
  cumulative_rewards_aius,
  cumulative_fees_aius
FROM final_data
ORDER BY day, model DESC;
