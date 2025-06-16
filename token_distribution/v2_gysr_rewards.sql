-- https://dune.com/queries/5245991/
WITH daily_sums AS (
  SELECT
    date_trunc('day', block_time) AS day,
    SUM(varbinary_to_uint256(bytearray_substring(data, 1, 32))) AS daily_amount
  FROM ethereum.logs
  WHERE contract_address = 0xa8f103eecfb619358c35f98c9372b31c64d3f4a1
    AND topic0 = 0x1a4dfb075362880d700ede1cc31d284b1c3b2811e9f0b2ddde7bdb270042c13f
    AND topic2 = 0x0000000000000000000000008afe4055ebc86bd2afb3940c0095c9aca511d852
  GROUP BY 1
),
min_day AS (
  SELECT MIN(day) AS first_day
  FROM daily_sums
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
    COALESCE(s.daily_amount, 0) AS daily_amount
  FROM date_series ds
  LEFT JOIN daily_sums s ON ds.day = s.day
)
SELECT
  day,
  CAST(daily_amount / POWER(10, 18) AS DECIMAL(38,18)) AS daily_amount,
  CAST(SUM(daily_amount) OVER (ORDER BY day) / POWER(10, 18) AS DECIMAL(38,18)) AS cumulative_amount
FROM all_days
ORDER BY day;
