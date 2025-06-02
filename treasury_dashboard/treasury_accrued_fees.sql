-- https://dune.com/queries/5220360
WITH treasury_history AS (
  SELECT
    evt_block_time,
    "to" AS treasury,
    LEAD(evt_block_time) OVER (ORDER BY evt_block_time) AS next_change_time
  FROM arbius_arbitrum.v2_enginev5_1_evt_treasurytransferred
),
withdraw_calls AS (
  SELECT
    c.call_block_time,
    c.call_tx_hash,
    th.treasury
  FROM arbius_arbitrum.v2_enginev5_1_call_withdrawaccruedfees c
  JOIN treasury_history th
    ON c.call_block_time >= th.evt_block_time
    AND (th.next_change_time IS NULL OR c.call_block_time < th.next_change_time)
  WHERE c.call_success = true
),
withdrawal_transfers AS (
  SELECT
    t.evt_block_time AS time,
    t.evt_tx_hash AS tx_hash,
    t.evt_index AS index,
    -t.value AS delta,
    'withdrawal' AS event_type,
    t.value AS withdrawn_amount
  FROM withdraw_calls w
  JOIN arbius_arbitrum.basetoken_evt_transfer t
    ON w.call_tx_hash = t.evt_tx_hash
    AND t."to" = w.treasury
),
accrual_events AS (
  SELECT
    evt_block_time AS time,
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    (treasuryFee + (remainingFee - validatorFee)) AS delta,
    'accrual' AS event_type,
    NULL AS withdrawn_amount
  FROM arbius_arbitrum.v2_enginev5_1_evt_feespaid
),
all_events AS (
  SELECT time, tx_hash, index, delta, event_type, withdrawn_amount
  FROM accrual_events
  UNION ALL
  SELECT time, tx_hash, index, delta, event_type, withdrawn_amount
  FROM withdrawal_transfers
),
events_with_accrued AS (
  SELECT
    *,
    LAG(accruedFees, 1, 0) OVER (ORDER BY time, tx_hash, index) AS prev_accruedFees
  FROM (
    SELECT
      *,
      SUM(delta) OVER (ORDER BY time, tx_hash, index) AS accruedFees
    FROM all_events
  ) sub
),
daily_timestamps AS (
  SELECT DISTINCT
    DATE_TRUNC('day', time) AS snapshot_time
  FROM events_with_accrued
),
recent_events AS (
  SELECT
    d.snapshot_time,
    e.time,
    e.tx_hash,
    e.index,
    e.accruedFees,
    ROW_NUMBER() OVER (
      PARTITION BY d.snapshot_time
      ORDER BY e.time DESC, e.tx_hash DESC, e.index DESC
    ) AS rn
  FROM daily_timestamps d
  LEFT JOIN events_with_accrued e
    ON e.time < d.snapshot_time
),
daily_snapshots AS (
  SELECT
    d.snapshot_time AS time,
    COALESCE(r.accruedFees, 0) AS accruedFees,
    'daily' AS event_type,
    NULL AS withdrawn_amount
  FROM daily_timestamps d
  LEFT JOIN recent_events r
    ON d.snapshot_time = r.snapshot_time
    AND r.rn = 1
),
withdrawal_snapshots AS (
  SELECT
    time,
    prev_accruedFees AS accruedFees,
    'pre-withdrawal' AS event_type,
    NULL AS withdrawn_amount
  FROM events_with_accrued
  WHERE event_type = 'withdrawal'
  UNION ALL
  SELECT
    time,
    0 AS accruedFees,
    'withdrawal' AS event_type,
    withdrawn_amount
  FROM events_with_accrued
  WHERE event_type = 'withdrawal'
),
final_data AS (
  SELECT
    time,
    accruedFees,
    event_type,
    withdrawn_amount
  FROM daily_snapshots
  UNION ALL
  SELECT
    time,
    accruedFees,
    event_type,
    withdrawn_amount
  FROM withdrawal_snapshots
)
SELECT
  time,
  CAST(accruedFees AS DOUBLE) / 1e18 AS accruedFees,
  event_type,
  CASE
    WHEN event_type = 'withdrawal' THEN CAST(withdrawn_amount AS DOUBLE) / 1e18
    ELSE NULL
  END AS withdrawn_amount
FROM final_data
ORDER BY time
