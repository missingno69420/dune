WITH date_range AS (
    SELECT MIN(evt_block_date) AS start_date, MAX(evt_block_date) AS end_date
    FROM (
        SELECT *
        FROM erc20_nova.evt_transfer
        WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
        AND "from" = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a
        UNION ALL
        SELECT *
        FROM erc20_nova.evt_transfer
        WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
        AND "to" = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a
    ) AS transfers
),
dates AS (
    SELECT CAST(time AS DATE) AS date
    FROM (
        SELECT sequence(
            start_date,
            end_date,
            interval '1' day
        ) AS date_array
        FROM date_range
    ) ts
    CROSS JOIN UNNEST(date_array) AS t(time)
),
daily_outflows AS (
    SELECT evt_block_date, CAST(SUM(value) AS DECIMAL(38,0)) AS daily_outflow
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND "from" = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a
    GROUP BY evt_block_date
),
daily_inflows AS (
    SELECT evt_block_date, CAST(SUM(value) AS DECIMAL(38,0)) AS daily_inflow
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND "to" = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a
    GROUP BY evt_block_date
),
cumulative_outflows AS (
    SELECT d.date,
           COALESCE(SUM(do.daily_outflow) OVER (ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_outflow
    FROM dates d
    LEFT JOIN daily_outflows do ON d.date = do.evt_block_date
),
cumulative_inflows AS (
    SELECT d.date,
           COALESCE(SUM(di.daily_inflow) OVER (ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_inflow
    FROM dates d
    LEFT JOIN daily_inflows di ON d.date = di.evt_block_date
)
SELECT
    dates.date,
    cumulative_outflow / 1e18 AS cumulative_outflow_tokens,
    cumulative_inflow / 1e18 AS cumulative_inflow_tokens,
    (cumulative_outflow - cumulative_inflow) / 1e18 AS cumulative_diff
FROM dates
JOIN cumulative_inflows ON dates.date = cumulative_inflows.date
JOIN cumulative_outflows ON dates.date = cumulative_outflows.date
ORDER BY dates.date
