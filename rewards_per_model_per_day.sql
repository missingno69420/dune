-- Tracking rewards and fees is tricky because RewardsPaid events do not have
-- an identifier that relates back to a task and model. To determine which model the rewards are
-- related to from solutions that are claimed we need to look to the transaction's event log and find the SolutionClaimed event
-- that comes before a RewardsPaid event. For rewards from nay-vote contestations we need to look for
-- RewardsPaid event that comes before a ContestationVoteFinish event. Furthermore, SolutionClaimed
-- events always have a RewardsPaid event after them in the event log, but ContestationVoteFinish may or may not
-- have RewardsPaid event before them in the event log. We can find a model id through a task through either ContestationVoteFinish
-- or SolutionClaimed event for every RewardsPaid event.
WITH all_events AS (
  -- Combine RewardsPaid, SolutionClaimed, and ContestationVoteFinish events
  SELECT
    tx_hash,
    index, -- Log index to order events within transaction
    block_time,
    'RewardsPaid' AS event_type,
    NULL AS task_id, -- RewardsPaid has no task_id
    varbinary_to_uint256(substr(data, 1+32*1, 32)) / 1e18 AS treasury_emission, -- treasuryReward
    varbinary_to_uint256(substr(data, 1+32*2, 32)) / 1e18 AS task_owner_emission, -- taskOwnerReward
    varbinary_to_uint256(substr(data, 1+32*3, 32)) / 1e18 AS validators_emission -- validatorReward
  FROM arbitrum.logs
  WHERE contract_address = 0x9b51ef044d3486a1fb0a2d55a6e0ceeadd323e66
    AND topic0 = 0x65ee2a4c05d8c4801cd9a8d8e592d0d3507a0f362400d63658e3e31aad1622e5 -- RewardsPaid event
    -- Removed time filter to capture all events
  UNION ALL
  SELECT
    tx_hash,
    index,
    block_time,
    'SolutionClaimed' AS event_type,
    topic2 AS task_id, -- taskid from SolutionClaimed
    NULL AS treasury_emission,
    NULL AS task_owner_emission,
    NULL AS validators_emission
  FROM arbitrum.logs
  WHERE contract_address = 0x9b51ef044d3486a1fb0a2d55a6e0ceeadd323e66
    AND topic0 = 0x0b76b4ae356796814d36b46f7c500bbd27b2cce1e6059a6fa2bebfd5a389b190 -- SolutionClaimed event
    -- Removed time filter
  UNION ALL
  SELECT
    tx_hash,
    index,
    block_time,
    'ContestationVoteFinish' AS event_type,
    topic1 AS task_id, -- taskid from ContestationVoteFinish
    NULL AS treasury_emission,
    NULL AS task_owner_emission,
    NULL AS validators_emission
  FROM arbitrum.logs
  WHERE contract_address = 0x9b51ef044d3486a1fb0a2d55a6e0ceeadd323e66
    AND topic0 = 0x6c2b86e3c3cdac246696e75a0971a43e3b2e8d4a423c842b806df7d6b3eb1e71 -- ContestationVoteFinish event
    -- Removed time filter
),
events_with_task_id AS (
  SELECT
    date_trunc('day', block_time) AS day,
    tx_hash,
    index,
    event_type,
    treasury_emission,
    task_owner_emission,
    validators_emission,
    -- Propagate SolutionClaimed task_id forward to next RewardsPaid
    LAG(CASE WHEN event_type = 'SolutionClaimed' THEN task_id END)
      OVER (PARTITION BY tx_hash ORDER BY index) AS solution_task_id,
    -- Propagate ContestationVoteFinish task_id backward to previous RewardsPaid
    LEAD(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END)
      OVER (PARTITION BY tx_hash ORDER BY index) AS contestation_task_id
  FROM all_events
),
rewards_with_tasks AS (
  SELECT
    day,
    tx_hash,
    index,
    treasury_emission,
    task_owner_emission, -- Corrected from 'task OWNER_emission'
    validators_emission,
    COALESCE(
      solution_task_id, -- Prefer SolutionClaimed task_id if available
      contestation_task_id -- Fallback to ContestationVoteFinish task_id
    ) AS task_id
  FROM events_with_task_id
  WHERE event_type = 'RewardsPaid' -- Only keep RewardsPaid events
),
task_models AS (
  SELECT
    topic1 AS task_id, -- taskid from TaskSubmitted (topic1)
    topic2 AS model_id -- model_id from TaskSubmitted (topic2)
  FROM arbitrum.logs
  WHERE contract_address = 0x9b51ef044d3486a1fb0a2d55a6e0ceeadd323e66
    AND topic0 = 0xc3d3e0544c80e3bb83f62659259ae1574f72a91515ab3cae3dd75cf77e1b0aea -- TaskSubmitted event
    -- Removed time filter
),
rewards_with_models AS (
  SELECT
    r.day,
    m.model_id,
    r.treasury_emission,
    r.task_owner_emission,
    r.validators_emission
  FROM rewards_with_tasks r
  LEFT JOIN task_models m ON r.task_id = m.task_id -- Link taskid to model_id
)
SELECT
  r.day, -- Date of rewards
  COALESCE(m.model_name, to_hex(r.model_id), 'No Task') AS model, -- Model name, hex ID, or 'No Task' for unmatched rewards
  SUM(r.treasury_emission) AS treasury_emission, -- Sum daily treasury rewards in tokens
  SUM(r.task_owner_emission) AS task_owner_emission, -- Sum daily task owner rewards in tokens
  SUM(r.validators_emission) AS validators_emission -- Sum daily validator rewards in tokens
FROM rewards_with_models r
LEFT JOIN query_5169304 m ON r.model_id = m.model_id -- Join with model mapping table for names
GROUP BY r.day, COALESCE(m.model_name, to_hex(r.model_id), 'No Task') -- Group by day and model to aggregate rewards
ORDER BY r.day, COALESCE(m.model_name, to_hex(r.model_id), 'No Task') -- Order by date and model for display
