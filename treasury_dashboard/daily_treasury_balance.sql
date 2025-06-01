-- https://dune.com/queries/5216894
WITH current_treasury AS (
  SELECT treasury_wallet
  FROM query_5216830
),
transfers AS (
  SELECT
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
daily_net_changes AS (
  SELECT
    DATE(block_time) AS day,
    wallet,
    SUM(net_change) AS daily_net_change
  FROM transfers
  GROUP BY DATE(block_time), wallet
),
cumulative_balances AS (
  SELECT
    day,
    wallet,
    SUM(daily_net_change) OVER (PARTITION BY wallet ORDER BY day) AS balance
  FROM daily_net_changes
)
SELECT
  day,
  wallet,
  balance / 1e18 AS balance_aius
FROM cumulative_balances
ORDER BY day DESC;
