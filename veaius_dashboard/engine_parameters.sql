-- https://dune.com/queries/5225137
WITH latest_paused AS (
  SELECT paused
  FROM arbius_arbitrum.engine_evt_pausedchanged
  ORDER BY evt_block_number DESC, evt_index DESC
  LIMIT 1
),
latest_version AS (
  SELECT version
  FROM arbius_arbitrum.engine_evt_versionchanged
  ORDER BY evt_block_number DESC, evt_index DESC
  LIMIT 1
),
latest_solution_model_fee_percentage AS (
  SELECT solutionModelFeePercentage_
  FROM arbius_arbitrum.engine_call_setsolutionmodelfeepercentage
  WHERE call_success = true
  ORDER BY call_block_number DESC, call_tx_index DESC
  LIMIT 1
),
latest_treasury AS (
  SELECT to AS treasury
  FROM arbius_arbitrum.engine_evt_treasurytransferred
  ORDER BY evt_block_number DESC, evt_index DESC
  LIMIT 1
),
latest_pauser AS (
  SELECT to AS pauser
  FROM arbius_arbitrum.engine_evt_pausertransferred
  ORDER BY evt_block_number DESC, evt_index DESC
  LIMIT 1
),
latest_start_block_time AS (
  SELECT startBlockTime_
  FROM arbius_arbitrum.engine_call_setstartblocktime
  WHERE call_success = true
  ORDER BY call_block_number DESC, call_tx_index DESC
  LIMIT 1
)
SELECT
  COALESCE((SELECT paused FROM latest_paused), false) AS paused,
  COALESCE((SELECT version FROM latest_version), 5) AS version,
  COALESCE(
    (SELECT (CAST(solutionModelFeePercentage_ AS DECIMAL(38,0)) / POWER(10,18)) * 100 FROM latest_solution_model_fee_percentage),
    (CAST(1000000000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100
  ) AS solution_model_fee_percentage,
  (CAST(2400000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100 AS validator_minimum_percentage,
  (CAST(10000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100 AS slash_amount_percentage,
  (CAST(100000000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100 AS solution_fee_percentage,
  (CAST(100000000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100 AS retraction_fee_percentage,
  (CAST(100000000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100 AS treasury_reward_percentage,
  (CAST(100000000000000000 AS DECIMAL(38,0)) / POWER(10,18)) * 100 AS task_owner_reward_percentage,
  COALESCE((SELECT treasury FROM latest_treasury), NULL) AS treasury,
  COALESCE((SELECT pauser FROM latest_pauser), NULL) AS pauser,
  COALESCE((SELECT startBlockTime_ FROM latest_start_block_time), 0) AS start_block_time
