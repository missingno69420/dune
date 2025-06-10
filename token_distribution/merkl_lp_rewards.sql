-- https://dune.com/queries/5264546/
WITH daily_sums AS (
  SELECT
    evt_block_date AS day,
    SUM(amount) AS daily_amount
  FROM merkl_ethereum.distributor_evt_claimed
  WHERE token = 0x8AFE4055Ebc86Bd2AFB3940c0095C9aca511d852
  GROUP BY evt_block_date
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
