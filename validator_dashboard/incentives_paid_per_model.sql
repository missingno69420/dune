-- https://dune.com/queries/5192138
WITH incentive_events AS (
  SELECT
    evt_block_time,
    taskid,
    amount
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded
),
tasks AS (
  SELECT
    id AS task_id,
    model AS model_id
  FROM arbius_arbitrum.engine_evt_tasksubmitted
),
incentives_with_models AS (
  SELECT
    date_trunc('day', i.evt_block_time) AS day,
    t.model_id,
    SUM(i.amount) / 1e18 AS total_incentive
  FROM incentive_events i
  JOIN tasks t ON i.taskid = t.task_id
  GROUP BY date_trunc('day', i.evt_block_time), t.model_id
),
model_mapping AS (
  SELECT
    model_id,
    model_name
  FROM query_5169304
)
SELECT
  i.day,
  COALESCE(m.model_name, to_hex(i.model_id)) AS model,
  i.total_incentive
FROM incentives_with_models i
LEFT JOIN model_mapping m ON i.model_id = m.model_id
ORDER BY i.day DESC, model
