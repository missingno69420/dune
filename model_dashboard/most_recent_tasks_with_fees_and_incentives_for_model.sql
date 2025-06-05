-- https://dune.com/queries/5240220/
WITH last_n_tasks AS (
    SELECT
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_tx_index AS tx_index,
        evt_index AS log_index,
        id AS task_id,
        fee / POWER(10, 18) AS task_fee_aius,
        ROW_NUMBER() OVER (ORDER BY evt_block_time DESC, evt_tx_index DESC, evt_index DESC) AS rank
    FROM arbius_arbitrum.engine_evt_tasksubmitted
    WHERE model = {{model_id}}
    ORDER BY evt_block_time DESC, evt_tx_index DESC, evt_index DESC
    LIMIT {{recent_tasks_length}}
),
incentives_total AS (
    SELECT
        l.task_id,
        COALESCE(SUM(ia.amount) / POWER(10, 18), 0) AS total_incentives_aius
    FROM last_n_tasks l
    LEFT JOIN arbius_arbitrum.arbiusrouterv1_evt_incentiveadded ia
        ON l.task_id = ia.taskid
    GROUP BY l.task_id
),
tasks_with_details AS (
    SELECT
        l.rank,
        l.task_id,
        l.block_time,
        l.tx_hash,
        l.task_fee_aius,
        i.total_incentives_aius
    FROM last_n_tasks l
    LEFT JOIN incentives_total i ON l.task_id = i.task_id
)
SELECT
    rank,
    'TaskSubmitted' AS event_type,
    task_id,
    block_time,
    tx_hash,
    'Fee' AS component_type,
    task_fee_aius AS value_aius
FROM tasks_with_details
UNION ALL
SELECT
    rank,
    'TaskSubmitted' AS event_type,
    task_id,
    block_time,
    tx_hash,
    'Incentive' AS component_type,
    total_incentives_aius AS value_aius
FROM tasks_with_details
ORDER BY rank ASC, component_type ASC;
