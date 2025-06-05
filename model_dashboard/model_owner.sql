-- https://dune.com/queries/5230623
WITH register_calls AS (
  SELECT
    call_tx_hash,
    call_trace_address,
    addr_ AS initial_addr,
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
initial_addresses AS (
  SELECT
    r.id,
    c.initial_addr,
    r.evt_block_number,
    r.evt_index
  FROM registered_events r
  JOIN register_calls c
    ON r.evt_tx_hash = c.call_tx_hash
    AND r.event_order = c.call_order
),
address_changes AS (
  SELECT
    id,
    addr,
    evt_block_number,
    evt_index
  FROM arbius_arbitrum.engine_evt_modeladdrchanged
),
all_address_settings AS (
  SELECT
    id,
    initial_addr AS addr,
    evt_block_number,
    evt_index
  FROM initial_addresses
  UNION ALL
  SELECT
    id,
    addr,
    evt_block_number,
    evt_index
  FROM address_changes
),
current_addresses AS (
  SELECT
    id,
    addr,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY evt_block_number DESC, evt_index DESC) AS rn
  FROM all_address_settings
)
SELECT
  to_hex(id) AS model_id,
  to_hex(addr) AS current_owner
FROM current_addresses
WHERE rn = 1
ORDER BY model_id;
