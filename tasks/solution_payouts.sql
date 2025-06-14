-- https://dune.com/queries/5284860/
WITH payout_data AS (
  SELECT
    CAST(COALESCE(f.modelFee - f.treasuryFee, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS model_owner_fee_aius,
    CAST(COALESCE(f.treasuryFee + (f.remainingFee - f.validatorFee), 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS treasury_fee_aius,
    CAST(COALESCE(f.validatorFee, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS validator_fee_aius,
    CAST(COALESCE(r.treasuryReward, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS treasury_reward_aius,
    CAST(COALESCE(r.taskOwnerReward, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS task_owner_reward_aius,
    CAST(COALESCE(r.validatorReward, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS validator_reward_aius,
    CAST(COALESCE(i.amount, 0) AS DECIMAL(38, 0)) / POWER(10, 18) AS incentive_paid_aius
  FROM arbius_arbitrum.engine_evt_tasksubmitted t
  LEFT JOIN arbius_arbitrum.engine_evt_solutionclaimed s ON t.id = s.task
  LEFT JOIN arbius_arbitrum.engine_evt_feespaid f ON s.evt_tx_hash = f.evt_tx_hash AND f.evt_index = s.evt_index + 1
  LEFT JOIN arbius_arbitrum.engine_evt_rewardspaid r ON s.evt_tx_hash = r.evt_tx_hash AND r.evt_index = s.evt_index + 2
  LEFT JOIN arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed i ON t.id = i.taskid
  WHERE t.id = {{task_id}}
)
SELECT 'Model Owner Fee' AS type, model_owner_fee_aius AS amount FROM payout_data
UNION ALL
SELECT 'Treasury Fee' AS type, treasury_fee_aius AS amount FROM payout_data
UNION ALL
SELECT 'Validator Fee' AS type, validator_fee_aius AS amount FROM payout_data
UNION ALL
SELECT 'Treasury Reward' AS type, treasury_reward_aius AS amount FROM payout_data
UNION ALL
SELECT 'Task Owner Reward' AS type, task_owner_reward_aius AS amount FROM payout_data
UNION ALL
SELECT 'Validator Reward' AS type, validator_reward_aius AS amount FROM payout_data
UNION ALL
SELECT 'Incentive Paid' AS type, incentive_paid_aius AS amount FROM payout_data
