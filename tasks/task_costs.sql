-- https://dune.com/queries/5284859/
WITH task_data AS (
  SELECT
    CAST(t.fee AS DECIMAL(38, 0)) / POWER(10, 18) AS task_fee_aius,
    CAST(COALESCE(ti.total_incentives_added, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS total_incentives_added_aius
  FROM arbius_arbitrum.engine_evt_tasksubmitted t
  LEFT JOIN (
    SELECT taskid, SUM(amount) AS total_incentives_added
    FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveadded
    GROUP BY taskid
  ) ti ON t.id = ti.taskid
  WHERE t.id = {{task_id}}
)
SELECT 'Task Fee' AS type, task_fee_aius AS amount FROM task_data
UNION ALL
SELECT 'Incentives Added' AS type, total_incentives_added_aius AS amount FROM task_data
