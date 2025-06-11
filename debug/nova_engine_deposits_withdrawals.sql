-- https://dune.com/queries/5270712/
WITH engine AS (
  SELECT 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a AS address
),
filtered_transfers AS (
  SELECT
    evt_block_time,
    "from",
    "to",
    value
  FROM arbius_nova.basetoken_evt_transfer
  WHERE "from" = (SELECT address FROM engine)
     OR "to" = (SELECT address FROM engine)
),
min_day AS (
  SELECT MIN(date_trunc('day', evt_block_time)) AS first_day
  FROM filtered_transfers
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
daily_deposits AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(value / 1000000000000000000) AS deposit_integer,
    SUM(CAST(value % 1000000000000000000 AS DOUBLE) / 1000000000000000000) AS deposit_fraction
  FROM filtered_transfers
  WHERE "to" = (SELECT address FROM engine)
  GROUP BY 1
),
daily_withdrawals AS (
  SELECT
    date_trunc('day', evt_block_time) AS day,
    SUM(value / 1000000000000000000) AS withdrawal_integer,
    SUM(CAST(value % 1000000000000000000 AS DOUBLE) / 1000000000000000000) AS withdrawal_fraction
  FROM filtered_transfers
  WHERE "from" = (SELECT address FROM engine)
  GROUP BY 1
),
all_days AS (
  SELECT
    ds.day,
    COALESCE(d.deposit_integer, 0) AS deposit_integer,
    COALESCE(d.deposit_fraction, 0) AS deposit_fraction,
    COALESCE(w.withdrawal_integer, 0) AS withdrawal_integer,
    COALESCE(w.withdrawal_fraction, 0) AS withdrawal_fraction
  FROM date_series ds
  LEFT JOIN daily_deposits d ON ds.day = d.day
  LEFT JOIN daily_withdrawals w ON ds.day = w.day
)
SELECT
  day,
  (deposit_integer + deposit_fraction) AS daily_deposits,
  (withdrawal_integer + withdrawal_fraction) AS daily_withdrawals,
  (SUM(deposit_integer) OVER (ORDER BY day) + SUM(deposit_fraction) OVER (ORDER BY day)) AS cumulative_deposits,
  -1 * (SUM(withdrawal_integer) OVER (ORDER BY day) + SUM(withdrawal_fraction) OVER (ORDER BY day)) AS cumulative_withdrawals
FROM all_days
ORDER BY day;
