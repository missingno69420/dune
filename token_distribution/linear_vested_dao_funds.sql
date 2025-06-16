-- https://dune.com/queries/5292842/
WITH vesting_params AS (
    SELECT
        DATE '2024-02-14' AS start_date,
        100000 AS total_tokens,
        365.25 * 4 AS vesting_days -- 4 years in days (accounting for leap years)
),

daily_vesting AS (
    SELECT
        start_date + INTERVAL '1' DAY * d.day_offset AS vesting_date,
        total_tokens / vesting_days AS daily_vesting_amount,
        total_tokens
    FROM vesting_params
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER () - 1 AS day_offset
        FROM UNNEST(SEQUENCE(0, 1460)) AS t(day_num) -- 4 years = ~1461 days
    ) d
    WHERE start_date + INTERVAL '1' DAY * d.day_offset <= CURRENT_DATE
),

cumulative_vesting AS (
    SELECT
        vesting_date,
        daily_vesting_amount,
        SUM(daily_vesting_amount) OVER (ORDER BY vesting_date) AS cumulative_vested,
        total_tokens
    FROM daily_vesting
)

SELECT
    vesting_date,
    daily_vesting_amount,
    CASE
        WHEN cumulative_vested >= total_tokens THEN total_tokens
        ELSE cumulative_vested
    END AS cumulative_vested
FROM cumulative_vesting
WHERE cumulative_vested <= total_tokens
ORDER BY vesting_date;
