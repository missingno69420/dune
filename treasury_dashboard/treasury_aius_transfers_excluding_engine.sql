WITH target_tx AS (
  SELECT evt_block_number, evt_tx_index
  FROM erc20_arbitrum.evt_transfer
  WHERE evt_tx_hash = 0x25d6486d8f4c55b875546b573239e0e367c95eac1b27017af3cce986bd24495e
),
current_treasury AS (
  SELECT treasury_wallet FROM query_5216830
),
transfers AS (
  SELECT
    t.evt_block_time,
    t.evt_block_number,
    t.evt_tx_index,
    t.evt_tx_hash,
    t."from" AS from_address,
    t."to" AS to_address,
    t.value / 1e18 AS amount_aius,
    CASE
      WHEN t."from" = ct.treasury_wallet THEN 'Out'
      ELSE 'In'
    END AS direction
  FROM erc20_arbitrum.evt_transfer t
  CROSS JOIN current_treasury ct
  WHERE
    t.contract_address = 0x4a24B101728e07A52053c13FB4dB2BcF490CAbc3
    AND (
      t."from" = ct.treasury_wallet
      OR
      t."to" = ct.treasury_wallet
    )
    AND NOT (
      (t."from" = ct.treasury_wallet AND t."to" = 0x9b51ef044d3486a1fb0a2d55a6e0ceeadd323e66)
      OR
      (t."to" = ct.treasury_wallet AND t."from" = 0x9b51ef044d3486a1fb0a2d55a6e0ceeadd323e66)
    )
    AND (t.evt_block_number, t.evt_tx_index) >= (SELECT evt_block_number, evt_tx_index FROM target_tx)
)
SELECT
  evt_block_time AS block_time,
  evt_tx_hash AS transaction_hash,
  from_address,
  to_address,
  amount_aius,
  direction
FROM transfers
ORDER BY evt_block_number DESC, evt_tx_index ASC;
