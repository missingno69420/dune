-- https://dune.com/queries/5192052/8543513
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
time_intervals AS (
  SELECT
    time AS interval_start,
    LEAD(time, 1, (SELECT end_time FROM params)) OVER (ORDER BY time) AS interval_end
  FROM time_series
),
tasks AS (
  SELECT
    id AS task_id,
    model AS model_id,
    fee AS task_fee,
    evt_block_time AS submission_time
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  WHERE evt_block_time >= (SELECT start_time FROM params)
    AND evt_block_time <= (SELECT end_time FROM params)
),
incentives AS (
  SELECT
    taskid AS task_id,
    SUM(amount) AS total_incentives
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded
  GROUP BY taskid
),
task_totals AS (
  SELECT
    t.task_id,
    t.model_id,
    t.submission_time,
    t.task_fee + COALESCE(i.total_incentives, 0) AS total_amount
  FROM tasks t
  LEFT JOIN incentives i ON t.task_id = i.task_id
),
task_intervals AS (
  SELECT
    ti.interval_start,
    tt.model_id,
    tt.total_amount
  FROM task_totals tt
  JOIN time_intervals ti
    ON tt.submission_time >= ti.interval_start
    AND tt.submission_time < ti.interval_end
),
sums_per_interval AS (
  SELECT
    interval_start,
    model_id,
    SUM(total_amount) AS sum_amount
  FROM task_intervals
  GROUP BY interval_start, model_id
),
model_min_max AS (
  SELECT
    model_id,
    MIN(sum_amount) AS min_sum,
    MAX(sum_amount) AS max_sum
  FROM sums_per_interval
  GROUP BY model_id
)
SELECT
  spi.interval_start AS datetime,
  COALESCE(m.model_name, TO_HEX(spi.model_id)) AS model,
  spi.sum_amount / POWER(10, 18) AS sum_amount_aius,
  mmm.min_sum / POWER(10, 18) AS min_sum_aius,
  mmm.max_sum / POWER(10, 18) AS max_sum_aius
FROM sums_per_interval spi
JOIN model_min_max mmm ON spi.model_id = mmm.model_id
LEFT JOIN query_5169304 m ON spi.model_id = m.model_id
ORDER BY spi.interval_start, spi.model_id;
