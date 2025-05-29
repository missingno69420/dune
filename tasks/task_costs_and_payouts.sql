-- https://dune.com/queries/5206095
SELECT
  t.fee AS task_fee,
  COALESCE(f.modelFee - f.treasuryFee, 0) AS model_owner_fee,
  COALESCE(f.treasuryFee + (f.remainingFee - f.validatorFee), 0) AS treasury_fee,
  COALESCE(f.validatorFee, 0) AS validator_fee,
  COALESCE(r.treasuryReward, 0) AS treasury_reward,
  COALESCE(r.taskOwnerReward, 0) AS task_owner_reward,
  COALESCE(r.validatorReward, 0) AS validator_reward,
  COALESCE(i.amount, 0) AS incentive_paid
FROM arbius_arbitrum.v2_enginev5_1_evt_tasksubmitted t
LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_solutionclaimed s
  ON t.id = s.task
LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_feespaid f
  ON s.evt_tx_hash = f.evt_tx_hash AND f.evt_index = s.evt_index + 1
LEFT JOIN arbius_arbitrum.v2_enginev5_1_evt_rewardspaid r
  ON s.evt_tx_hash = r.evt_tx_hash AND r.evt_index = s.evt_index + 2
LEFT JOIN arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed i
  ON t.id = i.taskid
WHERE t.id = {{task_id}};
