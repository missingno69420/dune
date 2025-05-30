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
tasks AS (
  SELECT
    id AS task_id,
    model AS model_id,
    fee / POWER(10, 18) AS task_fee,
    evt_block_time AS submission_time
  FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted
  CROSS JOIN params p
  WHERE evt_block_time >= p.start_time AND evt_block_time <= p.end_time
),
incentive_buckets AS (
  SELECT
    taskid AS task_id,
    date_trunc('minute', evt_block_time) AS bucket_time,
    SUM(amount) / POWER(10, 18) AS bucket_amount
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded
  CROSS JOIN params p
  WHERE evt_block_time >= p.start_time AND evt_block_time <= p.end_time
  GROUP BY taskid, date_trunc('minute', evt_block_time)
),
task_totals AS (
  SELECT
    t.task_id,
    t.model_id AS model,
    t.task_fee,
    ib.bucket_time,
    SUM(COALESCE(ib.bucket_amount, 0)) OVER (
      PARTITION BY t.task_id
      ORDER BY ib.bucket_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_incentives
  FROM tasks t
  LEFT JOIN incentive_buckets ib
    ON t.task_id = ib.task_id
    AND ib.bucket_time >= t.submission_time
    AND ib.bucket_time <= (SELECT end_time FROM params)
),
time_task_states AS (
  SELECT
    ts.time AS T,
    t.task_id,
    t.model_id AS model,
    t.task_fee,
    COALESCE(MAX(tt.bucket_time), NULL) AS latest_bucket_time
  FROM time_series ts
  JOIN tasks t ON t.submission_time <= ts.time
  LEFT JOIN task_totals tt
    ON tt.task_id = t.task_id
    AND tt.bucket_time <= ts.time
  GROUP BY ts.time, t.task_id, t.model_id, t.task_fee
),
task_states_with_incentives AS (
  SELECT
    tts.T,
    tts.task_id,
    tts.model,
    tts.task_fee + COALESCE(tt.cumulative_incentives, 0) AS total
  FROM time_task_states tts
  LEFT JOIN task_totals tt
    ON tt.task_id = tts.task_id
    AND tt.bucket_time = tts.latest_bucket_time
),
rankings AS (
  SELECT
    T,
    model,
    task_id,
    total,
    ROW_NUMBER() OVER (PARTITION BY T, model ORDER BY total DESC) AS rank_max,
    ROW_NUMBER() OVER (PARTITION BY T, model ORDER BY total ASC) AS rank_min
  FROM task_states_with_incentives
),
results AS (
  SELECT
    T,
    model,
    MAX(CASE WHEN rank_max = 1 THEN task_id END) AS max_task_id,
    MAX(CASE WHEN rank_max = 1 THEN total END) AS max_total,
    MAX(CASE WHEN rank_min = 1 THEN task_id END) AS min_task_id,
    MAX(CASE WHEN rank_min = 1 THEN total END) AS min_total
  FROM rankings
  GROUP BY T, model
)
SELECT
  r.T AS datetime,
  FORMAT_DATETIME(r.T, 'HH:mm:ss') AS time,
  COALESCE(m.model_name, TO_HEX(r.model)) AS model,
  r.max_task_id,
  r.max_total,
  r.min_task_id,
  r.min_total
FROM results r
LEFT JOIN query_5169304 m ON r.model = m.model_id
ORDER BY r.T, r.model;
