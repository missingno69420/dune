-- https://dune.com/queries/5191530
SELECT
  to_hex(e.id) AS model_id,
  COALESCE(m.model_name, '') AS model_name
FROM arbius_arbitrum.v2_enginev5_1_evt_modelregistered e
LEFT JOIN query_5169304 m ON e.id = m.model_id
ORDER BY e.evt_block_time ASC
