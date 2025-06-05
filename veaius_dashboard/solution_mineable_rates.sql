-- https://dune.com/queries/5216421/
WITH latest_rates AS (
  SELECT
    id AS model_id,
    rate,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY evt_block_number DESC, evt_index DESC) AS rn
  FROM arbius_arbitrum.engine_evt_solutionmineableratechange
)
SELECT
  m.model_name,
  CAST(CAST(lr.rate AS DECIMAL(38, 0)) / POWER(10, 16) AS DECIMAL(38, 18)) AS rate_percentage,
  TO_HEX(lr.model_id) AS model_id
FROM latest_rates lr
LEFT JOIN query_5169304 m ON lr.model_id = m.model_id
WHERE lr.rn = 1
ORDER BY lr.model_id;
