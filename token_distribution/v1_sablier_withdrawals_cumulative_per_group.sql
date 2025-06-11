-- https://dune.com/queries/5269610/
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
withdrawals AS (
  SELECT
    DATE(evt_block_time) AS day,
    CASE
      WHEN streamId IN (8041, 8042, 8043, 8044) THEN 'Private Sale'
      ELSE 'Team'
    END AS withdrawal_group,
    SUM(CAST(amount AS DOUBLE PRECISION)) / 1e18 AS daily_withdrawn
  FROM sablier_lockup_v1_1_multichain.sablierv2lockuplinear_evt_withdrawfromlockupstream
  WHERE asset = from_hex('e3dbc4f88eaa632ddf9708732e2832eeaa6688ab')
    AND streamId BETWEEN 8041 AND 8056
  GROUP BY DATE(evt_block_time),
           CASE WHEN streamId IN (8041, 8042, 8043, 8044) THEN 'Private Sale' ELSE 'Team' END
),
grid AS (
  SELECT d.day, g.withdrawal_group
  FROM date_series d
  CROSS JOIN (SELECT 'Private Sale' AS withdrawal_group UNION ALL SELECT 'Team' AS withdrawal_group) g
)
SELECT
  g.day,
  g.withdrawal_group,
  COALESCE(w.daily_withdrawn, 0) AS daily_withdrawn,
  SUM(COALESCE(w.daily_withdrawn, 0)) OVER (
    PARTITION BY g.withdrawal_group
    ORDER BY g.day
    ROWS UNBOUNDED PRECEDING
  ) AS cumulative_withdrawn
FROM grid g
LEFT JOIN withdrawals w ON g.day = w.day AND g.withdrawal_group = w.withdrawal_group
ORDER BY g.day, g.withdrawal_group
