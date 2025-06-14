-- https://dune.com/queries/5284859/
WITH task_fee AS (
  SELECT fee AS task_fee
  FROM arbius_arbitrum.engine_evt_tasksubmitted
  WHERE id = {{task_id}}
),
total_incentives AS (
  SELECT SUM(amount) AS total_incentives_added
  FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded
  WHERE taskid = {{task_id}}
)
SELECT 'Task Fee' AS type,
       COALESCE(CAST(CAST(task_fee AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM task_fee
UNION ALL
SELECT 'Incentives Added' AS type,
       COALESCE(CAST(CAST(total_incentives_added AS VARCHAR) AS DECIMAL(38,18)) / POWER(10,18), 0) AS amount
FROM total_incentives
