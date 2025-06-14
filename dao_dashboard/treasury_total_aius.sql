-- https://dune.com/queries/5223516
SELECT
  (SELECT balance_aius FROM query_5216894 ORDER BY time DESC LIMIT 1) +
  (SELECT accruedFees FROM query_5220360 ORDER BY time DESC LIMIT 1) AS total_aius
