-- https://dune.com/queries/5262204
WITH daily_sums AS (
  SELECT
    contract_address,
    date_trunc('day', block_time) AS day,
    SUM(CAST(varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS DECIMAL(38,0))) AS daily_reward
  FROM arbitrum.logs
  WHERE (contract_address = 0x1c0a14cac52ebde9c724e5627162a90a26b85e15
         AND topic0 = 0x4d7828f2aa36030cc63cdea79ec646099aa121d019f1a90edb55078939fb84ea)
     OR (contract_address = 0x1c405a0263ff9cc34b285f00002eb862e84c5fd1
         AND topic0 = 0xe2403640ba68fed3a2f88b7557551d1993f84b99bb10ff833f0cf8db0c5e0486)
  GROUP BY contract_address, date_trunc('day', block_time)
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
contracts AS (
  SELECT DISTINCT contract_address
  FROM daily_sums
),
all_days AS (
  SELECT
    c.contract_address,
    ds.day,
    COALESCE(s.daily_reward, 0) AS daily_reward
  FROM contracts c
  CROSS JOIN date_series ds
  LEFT JOIN daily_sums s ON c.contract_address = s.contract_address AND ds.day = s.day
)
SELECT
  contract_address,
  day,
  CAST(daily_reward / POWER(10, 18) AS DECIMAL(38,18)) AS daily_reward,
  CAST(SUM(daily_reward) OVER (PARTITION BY contract_address ORDER BY day) / POWER(10, 18) AS DECIMAL(38,18)) AS cumulative_reward
FROM all_days
ORDER BY contract_address, day;
