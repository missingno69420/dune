-- https://dune.com/queries/5247542/
-- Step 1: Combine data from both sources with source labels
WITH all_rewards AS (
    SELECT day, cumulative_amount, 'V1 Gysr Rewards' AS source
    FROM query_5245571
    UNION ALL
    SELECT day, cumulative_amount, 'V2 Gysr Rewards' AS source
    FROM query_5245991
),
-- Step 2: Find the earliest day across both sources
min_day AS (
    SELECT MIN(day) AS first_day
    FROM all_rewards
),
-- Step 3: Generate a daily time series from the first day to today
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
-- Step 4: Create all day-source combinations
date_source AS (
    SELECT d.day, s.source
    FROM date_series d
    CROSS JOIN (SELECT DISTINCT source FROM all_rewards) s
),
-- Step 5: Find the latest day per day-source pair
latest_rewards AS (
    SELECT
        ds.day,
        ds.source,
        MAX(ar.day) AS latest_day
    FROM date_source ds
    LEFT JOIN all_rewards ar ON ar.source = ds.source AND ar.day <= ds.day
    GROUP BY ds.day, ds.source
)
-- Step 6: Retrieve the cumulative_amount for the latest day
SELECT
    lr.day,
    lr.source,
    COALESCE(ar.cumulative_amount, 0) AS cumulative_amount
FROM latest_rewards lr
LEFT JOIN all_rewards ar ON ar.source = lr.source AND ar.day = lr.latest_day
ORDER BY lr.day DESC, lr.source;
