-- https://dune.com/queries/5218733/
SELECT
    day,
    model,
    SUM(daily_rewards) OVER (PARTITION BY model ORDER BY day) AS cumulative_rewards_aius,
    SUM(daily_fees) OVER (PARTITION BY model ORDER BY day) AS cumulative_fees_aius
FROM (
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        COALESCE(m.model_name, TO_HEX(t.model_id)) AS model,
        SUM(COALESCE(treasury_reward, 0)) / 1e18 AS daily_rewards,
        SUM(COALESCE(treasury_total_fee, 0)) / 1e18 AS daily_fees
    FROM query_5179596 e
    LEFT JOIN (
        SELECT id as task_id, model AS model_id
        FROM arbius_arbitrum.engine_evt_tasksubmitted
    ) t
        ON e.task_id = t.task_id
    LEFT JOIN query_5169304 m
        ON t.model_id = m.model_id
    GROUP BY 1, 2
) daily
ORDER BY day, model DESC;
