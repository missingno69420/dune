-- https://dune.com/queries/5216767
SELECT * FROM (
  VALUES
    (CAST(100000000000000000 AS UINT256), CAST(100000000000000000 AS UINT256))
) AS t (treasury_reward_percentage, task_owner_reward_percentage);
