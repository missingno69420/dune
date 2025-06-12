-- https://dune.com/queries/5216830
SELECT "to" AS treasury_wallet
FROM arbius_arbitrum.engine_evt_treasurytransferred
ORDER BY evt_block_time DESC
LIMIT 1;
