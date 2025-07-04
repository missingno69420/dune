-- https://dune.com/queries/5247223/
WITH treasury_ethereum_v2_transfers AS (
  SELECT
    'Ethereum' AS chain,
    'treasury' AS wallet_type,
    'v2' AS version,
    t.evt_block_time AS block_time,
    0xf20d0ebd8223dff22cfaf05f0549021525015577 AS wallet,
    CASE
      WHEN t."from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_ethereum.evt_transfer t
  WHERE t.contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND (t."from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 OR t."to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577)
),
treasury_ethereum_v1_transfers AS (
  SELECT
    'Ethereum' AS chain,
    'treasury' AS wallet_type,
    'v1' AS version,
    t.evt_block_time AS block_time,
    0xf20d0ebd8223dff22cfaf05f0549021525015577 AS wallet,
    CASE
      WHEN t."from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_ethereum.evt_transfer t
  WHERE t.contract_address = 0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab
    AND (t."from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 OR t."to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577)
),
deployer_ethereum_v2_transfers AS (
  SELECT
    'Ethereum' AS chain,
    'deployer' AS wallet_type,
    'v2' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_ethereum.evt_transfer t
  WHERE t.contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
deployer_ethereum_v1_transfers AS (
  SELECT
    'Ethereum' AS chain,
    'deployer' AS wallet_type,
    'v1' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_ethereum.evt_transfer t
  WHERE t.contract_address = 0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
treasury_arbitrum_one_v2_transfers AS (
  SELECT
    'Arbitrum One' AS chain,
    'treasury' AS wallet_type,
    'v2' AS version,
    t.evt_block_time AS block_time,
    0xf20d0ebd8223dff22cfaf05f0549021525015577 AS wallet,
    CASE
      WHEN t."from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_arbitrum.evt_transfer t
  WHERE t.contract_address = 0x4a24b101728e07a52053c13fb4db2bcf490cabc3
    AND (t."from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 OR t."to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577)
),
deployer_arbitrum_one_v2_transfers AS (
  SELECT
    'Arbitrum One' AS chain,
    'deployer' AS wallet_type,
    'v2' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_arbitrum.evt_transfer t
  WHERE t.contract_address = 0x4a24b101728e07a52053c13fb4db2bcf490cabc3
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
treasury_nova_v2_transfers AS (
  SELECT
    'Arbitrum Nova' AS chain,
    'treasury' AS wallet_type,
    'v2' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_nova.evt_transfer t
  WHERE t.contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
deployer_nova_v2_transfers AS (
  SELECT
    'Arbitrum Nova' AS chain,
    'deployer' AS wallet_type,
    'v2' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_nova.evt_transfer t
  WHERE t.contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
treasury_nova_v1_transfers AS (
  SELECT
    'Arbitrum Nova' AS chain,
    'treasury' AS wallet_type,
    'v1' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_nova.evt_transfer t
  WHERE t.contract_address = 0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
deployer_nova_v1_transfers AS (
  SELECT
    'Arbitrum Nova' AS chain,
    'deployer' AS wallet_type,
    'v1' AS version,
    t.evt_block_time AS block_time,
    0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AS wallet,
    CASE
      WHEN t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN -CAST(t.value AS DOUBLE)
      WHEN t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN CAST(t.value AS DOUBLE)
    END AS net_change
  FROM erc20_nova.evt_transfer t
  WHERE t.contract_address = 0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab
    AND (t."from" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 OR t."to" = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
),
all_transfers AS (
  SELECT * FROM treasury_ethereum_v2_transfers
  UNION ALL
  SELECT * FROM treasury_ethereum_v1_transfers
  UNION ALL
  SELECT * FROM deployer_ethereum_v2_transfers
  UNION ALL
  SELECT * FROM deployer_ethereum_v1_transfers
  UNION ALL
  SELECT * FROM treasury_arbitrum_one_v2_transfers
  UNION ALL
  SELECT * FROM deployer_arbitrum_one_v2_transfers
  UNION ALL
  SELECT * FROM treasury_nova_v2_transfers
  UNION ALL
  SELECT * FROM deployer_nova_v2_transfers
  UNION ALL
  SELECT * FROM treasury_nova_v1_transfers
  UNION ALL
  SELECT * FROM deployer_nova_v1_transfers
),
daily_net_changes AS (
  SELECT
    DATE(block_time) AS day,
    chain,
    wallet_type,
    version,
    wallet,
    SUM(net_change) AS daily_net_change
  FROM all_transfers
  GROUP BY DATE(block_time), chain, wallet_type, version, wallet
),
min_days AS (
  SELECT chain, wallet_type, version, wallet, MIN(day) AS min_day
  FROM daily_net_changes
  GROUP BY chain, wallet_type, version, wallet
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
wallet_chain_version_date_series AS (
  SELECT md.chain, md.wallet_type, md.version, md.wallet, ds.day
  FROM min_days md
  CROSS JOIN date_series ds
  WHERE ds.day >= md.min_day
),
daily_balances AS (
  SELECT
    wcds.chain,
    wcds.wallet_type,
    wcds.version,
    wcds.wallet,
    wcds.day,
    COALESCE(dnc.daily_net_change, 0) AS daily_net_change
  FROM wallet_chain_version_date_series wcds
  LEFT JOIN daily_net_changes dnc
    ON wcds.chain = dnc.chain AND wcds.wallet_type = dnc.wallet_type AND wcds.version = dnc.version AND wcds.wallet = dnc.wallet AND wcds.day = dnc.day
),
cumulative_balances AS (
  SELECT
    chain,
    wallet_type,
    version,
    wallet,
    day,
    SUM(daily_net_change) OVER (PARTITION BY chain, wallet_type, version, wallet ORDER BY day) AS balance
  FROM daily_balances
)
SELECT
  day,
  wallet_type || '_' || chain || '_' || version AS wallet_chain_version,
  wallet,
  balance / 1e18 AS balance_aius
FROM cumulative_balances
ORDER BY day DESC, wallet_chain_version;
