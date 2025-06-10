-- https://dune.com/queries/5259180/
-- https://app.sablier.com/vesting/?t=search&c=1&a=0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab
WITH min_max_dates AS (
  SELECT
    MIN(DATE(evt_block_time)) AS min_date,
    MAX(DATE(evt_block_time)) AS max_date
  FROM sablier_lockup_v1_1_multichain.sablierv2lockuplinear_evt_withdrawfromlockupstream
  WHERE asset = from_hex('e3dbc4f88eaa632ddf9708732e2832eeaa6688ab')
    AND streamId BETWEEN 8041 AND 8056
),
date_series AS (
  SELECT
    DATE_ADD('day', t.num, m.min_date) AS day
  FROM
    min_max_dates m
  CROSS JOIN
    UNNEST(sequence(0, DATE_DIFF('day', m.min_date, m.max_date))) AS t(num)
),
stream_ids AS (
  SELECT DISTINCT streamId
  FROM sablier_lockup_v1_1_multichain.sablierv2lockuplinear_evt_withdrawfromlockupstream
  WHERE asset = from_hex('e3dbc4f88eaa632ddf9708732e2832eeaa6688ab')
    AND streamId BETWEEN 8041 AND 8056
),
grid AS (
  SELECT s.streamId, d.day
  FROM stream_ids s
  CROSS JOIN date_series d
),
withdrawals AS (
  SELECT
    streamId,
    DATE(evt_block_time) AS day,
    CAST(SUM(CAST(amount AS DOUBLE)) / 1e18 AS DOUBLE) AS daily_withdrawn
  FROM sablier_lockup_v1_1_multichain.sablierv2lockuplinear_evt_withdrawfromlockupstream
  WHERE asset = from_hex('e3dbc4f88eaa632ddf9708732e2832eeaa6688ab')
    AND streamId BETWEEN 8041 AND 8056
  GROUP BY streamId, DATE(evt_block_time)
)
SELECT
  g.streamId,
  g.day,
  COALESCE(w.daily_withdrawn, 0) AS daily_withdrawn,
  SUM(COALESCE(w.daily_withdrawn, 0)) OVER (
    PARTITION BY g.streamId
    ORDER BY g.day
    ROWS UNBOUNDED PRECEDING
  ) AS cumulative_withdrawn
FROM grid g
LEFT JOIN withdrawals w ON g.streamId = w.streamId AND g.day = w.day
ORDER BY g.streamId, g.day
