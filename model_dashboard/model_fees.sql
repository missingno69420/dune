-- https://dune.com/queries/5230506
WITH register_calls AS (
  SELECT
    call_tx_hash,
    call_trace_address,
    fee_ AS initial_fee,
    ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_trace_address) AS call_order
  FROM arbius_arbitrum.engine_call_registermodel
  WHERE call_success = TRUE
),
registered_events AS (
  SELECT
    evt_tx_hash,
    evt_index,
    evt_block_number,
    evt_block_time,
    id,
    ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS event_order
  FROM arbius_arbitrum.engine_evt_modelregistered
),
initial_fees AS (
  SELECT
    r.id,
    c.initial_fee,
    r.evt_block_number,
    r.evt_index
  FROM registered_events r
  JOIN register_calls c
    ON r.evt_tx_hash = c.call_tx_hash
    AND r.event_order = c.call_order
),
fee_changes AS (
  SELECT
    id,
    fee,
    evt_block_number,
    evt_index
  FROM arbius_arbitrum.engine_evt_modelfeechanged
),
all_fee_settings AS (
  SELECT
    id,
    initial_fee AS fee,
    evt_block_number,
    evt_index
  FROM initial_fees
  UNION ALL
  SELECT
    id,
    fee,
    evt_block_number,
    evt_index
  FROM fee_changes
),
current_fees AS (
  SELECT
    id,
    fee,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY evt_block_number DESC, evt_index DESC) AS rn
  FROM all_fee_settings
)
SELECT
  to_hex(id) AS model_id,
  fee AS model_fee
FROM current_fees
WHERE rn = 1
ORDER BY model_id;
