-- https://dune.com/queries/5247542/
-- Combine data from V1, V2 rewards, and the initial liquidity transfer
WITH all_rewards AS (
    SELECT day, cumulative_amount, 'V1 Gysr Rewards' AS source
    FROM query_5245571
    UNION ALL
    SELECT day, cumulative_amount, 'V2 Gysr Rewards' AS source
    FROM query_5245991
    UNION ALL
    SELECT day, cumulative_amount, 'Nova Engine Rewards' AS source
    FROM query_5247068
    UNION All
    SELECT day, cumulative_treasury_rewards_tokens as cumulative_amount, 'Rewards Paid to Treasury' AS source
    FROM query_5168325
    UNION All
    SELECT day, cumulative_task_owner_rewards_tokens as cumulative_amount, 'Rewards Paid to Task Owners' AS source
    FROM query_5168325
    UNION All
    SELECT day, cumulative_validator_rewards_tokens as cumulative_amount, 'Rewards Paid to Validators' AS source
    FROM query_5168325
    UNION All
    SELECT
        date_trunc('day', evt_block_time) AS day,
        value / 1e18 AS cumulative_amount,
        'Initial Dex Liquidity' AS source
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address = 0xe3DBC4F88EAa632DDF9708732E2832EEaA6688AB
      AND evt_tx_hash = 0xd2842d9e23b8452d4b43b4b112b826e23e60282ac72830eb7562fb51f60c27af
      AND evt_index = 1
),
-- Find the earliest day across all sources, including the transfer event
min_day AS (
    SELECT MIN(day) AS first_day
    FROM all_rewards
),
-- Generate a daily time series from the first day to today
date_series AS (
    SELECT time AS day
    FROM (
        SELECT sequence(
            CAST((SELECT first_day FROM min_day) AS DATE),
            CAST(date_trunc('day', NOW()) AS DATE),
            INTERVAL '1' DAY
        ) AS date_array
    ) ts
    CROSS JOIN UNNEST(date_array) AS t(time)
),
-- Create all day-source combinations
date_source AS (
    SELECT d.day, s.source
    FROM date_series d
    CROSS JOIN (SELECT DISTINCT source FROM all_rewards) s
),
-- Find the latest day per day-source pair
latest_rewards AS (
    SELECT
        ds.day,
        ds.source,
        MAX(ar.day) AS latest_day
    FROM date_source ds
    LEFT JOIN all_rewards ar ON ar.source = ds.source AND ar.day <= ds.day
    GROUP BY ds.day, ds.source
)
-- Retrieve the cumulative_amount for the latest day
SELECT
    lr.day,
    lr.source,
    COALESCE(ar.cumulative_amount, 0) AS cumulative_amount
FROM latest_rewards lr
LEFT JOIN all_rewards ar ON ar.source = lr.source AND ar.day = lr.latest_day
ORDER BY lr.day DESC, lr.source;
