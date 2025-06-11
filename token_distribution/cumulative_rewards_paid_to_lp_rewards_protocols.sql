-- https://dune.com/queries/5268057
WITH v1_gysr_fee AS (
    SELECT
        date_trunc('day', block_time) AS day,
        varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS fee_amount
    FROM ethereum.logs
    WHERE contract_address = 0x9b2f4eb5dd0e6e2087be155ba191da54cf1ae446
      AND topic0 = 0x6ded982279c8387ad8a63e73385031a3807c1862e633f06e09d11bcb6e282f60
      AND tx_hash = 0x51a78f506242e3808031234b81cd95ac042d1d791f9d48eb2481ab64c543abff
),
v2_gysr_fee AS (
    SELECT
        date_trunc('day', block_time) AS day,
        varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS fee_amount
    FROM ethereum.logs
    WHERE contract_address = 0xA8f103eEcfb619358C35F98c9372B31c64d3f4A1
      AND topic0 = 0x6ded982279c8387ad8a63e73385031a3807c1862e633f06e09d11bcb6e282f60
      AND tx_hash = 0x0e7fa04f11df392bbf832056a4f70dd210b625267f9c9ce3715edd7edf7ce8d7
),
combined_fees AS (
    SELECT
        day,
        SUM(fee_amount) AS daily_fee
    FROM (
        SELECT day, fee_amount FROM v1_gysr_fee
        UNION ALL
        SELECT day, fee_amount FROM v2_gysr_fee
    ) sub
    GROUP BY day
),
min_day AS (
    SELECT MIN(day) AS first_day
    FROM combined_fees
),
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
all_days AS (
    SELECT
        ds.day,
        COALESCE(cf.daily_fee, 0) AS daily_fee
    FROM date_series ds
    LEFT JOIN combined_fees cf ON ds.day = cf.day
)
SELECT
    day,
    'Gysr' AS payee,
    CAST(daily_fee / POWER(10, 18) AS DECIMAL(38,18)) AS daily_fee,
    CAST(SUM(daily_fee) OVER (ORDER BY day) / POWER(10, 18) AS DECIMAL(38,18)) AS cumulative_fee
FROM all_days
ORDER BY day;
