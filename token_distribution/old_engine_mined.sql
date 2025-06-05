WITH date_range AS (
    SELECT MIN(evt_block_date) AS start_date, MAX(evt_block_date) AS end_date
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND "from" = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a
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
    SELECT evt_block_date, SUM(value) AS daily_outflow
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND "from" = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a
    GROUP BY evt_block_date
),
cumulative_outflows AS (
    SELECT d.date,
           COALESCE(SUM(do.daily_outflow) OVER (ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_outflow
    FROM dates d
    LEFT JOIN daily_outflows do ON d.date = do.evt_block_date
)
SELECT
    date,
    cumulative_outflow / 1e18 AS cumulative_outflow_tokens
FROM cumulative_outflows
ORDER BY date
