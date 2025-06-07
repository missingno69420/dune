-- https://dune.com/queries/5247223/
WITH current_treasury AS (
  SELECT treasury_wallet
  FROM query_5216830
),
arbitrum_transfers AS (
  SELECT
    'Arbitrum' AS chain,
    t.evt_block_time AS block_time,
    ct.treasury_wallet AS wallet,
    CASE
      WHEN t."from" = ct.treasury_wallet THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = ct.treasury_wallet THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_arbitrum.evt_transfer t
  CROSS JOIN current_treasury ct
  WHERE t.contract_address = 0x4a24B101728e07A52053c13FB4dB2BcF490CAbc3
    AND (t."from" = ct.treasury_wallet OR t."to" = ct.treasury_wallet)
),
ethereum_transfers AS (
  SELECT
    'Ethereum' AS chain,
    t.evt_block_time AS block_time,
    ct.treasury_wallet AS wallet,
    CASE
      WHEN t."from" = ct.treasury_wallet THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = ct.treasury_wallet THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_ethereum.evt_transfer t
  CROSS JOIN current_treasury ct
  WHERE t.contract_address = 0x8AFE4055Ebc86Bd2AFB3940c0095C9aca511d852
    AND (t."from" = ct.treasury_wallet OR t."to" = ct.treasury_wallet)
),
all_transfers AS (
  SELECT * FROM arbitrum_transfers
  UNION ALL
  SELECT * FROM ethereum_transfers
),
daily_net_changes AS (
  SELECT
    DATE(block_time) AS day,
    chain,
    wallet,
    SUM(net_change) AS daily_net_change
  FROM all_transfers
  GROUP BY DATE(block_time), chain, wallet
),
min_days AS (
  SELECT chain, wallet, MIN(day) AS min_day
  FROM daily_net_changes
  GROUP BY chain, wallet
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
wallet_chain_date_series AS (
  SELECT md.chain, md.wallet, ds.day
  FROM min_days md
  CROSS JOIN date_series ds
  WHERE ds.day >= md.min_day
),
daily_balances AS (
  SELECT
    wcds.chain,
    wcds.wallet,
    wcds.day,
    COALESCE(dnc.daily_net_change, 0) AS daily_net_change
  FROM wallet_chain_date_series wcds
  LEFT JOIN daily_net_changes dnc
    ON wcds.chain = dnc.chain AND wcds.wallet = dnc.wallet AND wcds.day = dnc.day
),
cumulative_balances AS (
  SELECT
    chain,
    wallet,
    day,
    SUM(daily_net_change) OVER (PARTITION BY chain, wallet ORDER BY day) AS balance
  FROM daily_balances
)
SELECT
  day,
  chain,
  wallet,
  balance / 1e18 AS balance_aius
FROM cumulative_balances
ORDER BY day DESC, chain;
