-- https://dune.com/queries/5225137
SELECT * FROM (
  VALUES (
    CAST(2400000000000000 AS UINT256),     -- validatorMinimumPercentage
    CAST(10000000000000 AS UINT256),       -- slashAmountPercentage
    CAST(100000000000000000 AS UINT256),   -- solutionFeePercentage
    CAST(100000000000000000 AS UINT256),   -- retractionFeePercentage
    CAST(100000000000000000 AS UINT256),   -- treasuryRewardPercentage
    CAST(100000000000000000 AS UINT256),   -- taskOwnerRewardPercentage
    CAST(1000000000000000000 AS UINT256)  -- solutionModelFeePercentage
  )
) AS t (
  validator_minimum_percentage,
  slash_amount_percentage,
  solution_fee_percentage,
  retraction_fee_percentage,
  treasury_reward_percentage,
  task_owner_reward_percentage,
  solution_model_fee_percentage
);
