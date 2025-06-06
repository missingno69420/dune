WITH daily_sums AS (
  SELECT
    date_trunc('day', block_time) AS day,
    SUM(varbinary_to_uint256(bytearray_substring(data, 1, 32))) AS daily_amount
  FROM ethereum.logs
  WHERE contract_address = 0x9b2f4eb5dd0e6e2087be155ba191da54cf1ae446
    AND topic0 = 0x1a4dfb075362880d700ede1cc31d284b1c3b2811e9f0b2ddde7bdb270042c13f
    AND topic2 = 0x000000000000000000000000e3dbc4f88eaa632ddf9708732e2832eeaa6688ab
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
