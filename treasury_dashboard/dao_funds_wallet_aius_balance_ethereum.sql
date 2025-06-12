-- https://dune.com/queries/5274655
WITH transfers AS (
  SELECT
    evt_block_time,
    CAST(
      CASE
        WHEN "to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN value
        WHEN "from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577 THEN -value
        ELSE 0
      END AS DECIMAL(38,0)  -- Use decimal(38,0) instead of decimal(78,0)
    ) AS change
  FROM erc20_ethereum.evt_transfer
  WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
    AND ("from" = 0xf20d0ebd8223dff22cfaf05f0549021525015577
         OR "to" = 0xf20d0ebd8223dff22cfaf05f0549021525015577)
),
cumulative_balance AS (
  SELECT
    evt_block_time,
    SUM(change) OVER (
      ORDER BY evt_block_time
    ) AS balance  -- Result is decimal(38,0)
  FROM transfers
)
SELECT
  CAST(
    COALESCE(
      (SELECT balance
       FROM cumulative_balance
       WHERE evt_block_time < NOW()
       ORDER BY evt_block_time DESC
       LIMIT 1),
      0
    ) / POWER(10, 18) AS DECIMAL(38,18)  -- Convert wei to tokens
  ) AS balance_aius;
