-- https://dune.com/queries/5258236/
WITH current_engine AS (
  SELECT 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a as engine_wallet
  FROM query_5216830
),
transfers AS (
  SELECT
    t.evt_block_time AS block_time,
    ct.engine_wallet AS wallet,
    CASE
      WHEN t."from" = ct.engine_wallet THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = ct.engine_wallet THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_nova.evt_transfer t
  CROSS JOIN current_engine ct
  WHERE t.contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND (t."from" = ct.engine_wallet OR t."to" = ct.engine_wallet)
),
daily_net_changes AS (
  SELECT
    DATE(block_time) AS day,
    wallet,
    SUM(net_change) AS daily_net_change
  FROM transfers
  GROUP BY DATE(block_time), wallet
),
min_days AS (
  SELECT wallet, MIN(day) AS min_day
  FROM daily_net_changes
  GROUP BY wallet
),
overall_min_day AS (
  SELECT MIN(min_day) AS min_day
  FROM min_days
),
global_date_series AS (
  SELECT sequence(
    (SELECT min_day FROM overall_min_day),
    CURRENT_DATE,
    INTERVAL '1' DAY
  ) AS date_array
),
date_series AS (
  SELECT t.day
  FROM global_date_series gds
  CROSS JOIN UNNEST(gds.date_array) AS t(day)
),
wallet_date_series AS (
  SELECT md.wallet, ds.day
  FROM min_days md
  CROSS JOIN date_series ds
  WHERE ds.day >= md.min_day
),
daily_balances AS (
  SELECT
    wds.wallet,
    wds.day,
    COALESCE(dnc.daily_net_change, 0) AS daily_net_change
  FROM wallet_date_series wds
  LEFT JOIN daily_net_changes dnc
    ON wds.wallet = dnc.wallet AND wds.day = dnc.day
),
cumulative_balances AS (
  SELECT
    wallet,
    day,
    SUM(daily_net_change) OVER (PARTITION BY wallet ORDER BY day) AS balance
  FROM daily_balances
)
SELECT
  day,
  wallet,
  balance / 1e18 AS balance_aius
FROM cumulative_balances
ORDER BY day DESC;
