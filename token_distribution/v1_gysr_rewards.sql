-- https://dune.com/queries/5245571/
WITH daily_sums AS (
  SELECT
    date_trunc('day', block_time) AS day,
    SUM(varbinary_to_uint256(bytearray_substring(data, 1, 32))) AS daily_amount
  FROM ethereum.logs
  WHERE contract_address = 0x9b2f4eb5dd0e6e2087be155ba191da54cf1ae446
    AND topic0 = 0x1a4dfb075362880d700ede1cc31d284b1c3b2811e9f0b2ddde7bdb270042c13f
    AND topic2 = 0x000000000000000000000000e3dbc4f88eaa632ddf9708732e2832eeaa6688ab
  GROUP BY 1
)
SELECT
  day,
  CAST(daily_amount / POWER(10, 18) AS DECIMAL(38,18)) AS daily_amount,
  CAST(SUM(daily_amount) OVER (ORDER BY day) / POWER(10, 18) AS DECIMAL(38,18)) AS cumulative_amount
FROM daily_sums
ORDER BY day;
