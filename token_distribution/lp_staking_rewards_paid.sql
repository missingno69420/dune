-- https://dune.com/queries/5264349
WITH daily_sums AS (
  SELECT
    date_trunc('day', block_time) AS day,
    SUM(varbinary_to_uint256(bytearray_substring(data, 1, 32))) AS daily_rewards
  FROM ethereum.logs
  WHERE contract_address = 0x1c0a14cAC52ebDe9c724e5627162a90A26B85E15
    AND topic0 = 0xe2403640ba68fed3a2f88b7557551d1993f84b99bb10ff833f0cf8db0c5e0486
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
    COALESCE(s.daily_rewards, 0) AS daily_rewards
  FROM date_series ds
  LEFT JOIN daily_sums s ON ds.day = s.day
)
SELECT
  day,
  CAST(daily_rewards / POWER(10, 18) AS DECIMAL(38,18)) AS daily_rewards,
  CAST(SUM(daily_rewards) OVER (ORDER BY day) / POWER(10, 18) AS DECIMAL(38,18)) AS cumulative_rewards
FROM all_days
ORDER BY day;
