-- https://dune.com/queries/5247068
WITH task_fees AS (
  SELECT
    CAST(topic1 AS VARCHAR) AS task_id,
    bytea2numeric(data) AS fee
  FROM nova.logs
  WHERE topic0 = 0xc3d3e0544c80e3bb83f62659259ae1574f72a91515ab3cae3dd75cf77e1b0aea -- keccak256(TaskSubmitted(bytes32,bytes32,uint256,address))
    AND contract_address = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Replace with engine_address
),
solution_submitted AS (
  SELECT
    CAST(topic2 AS VARCHAR) AS task_id,
    CAST(topic1 AS VARCHAR) AS validator
  FROM nova.logs
  WHERE topic0 = 0x957c18b5af8413899ea8a576a4d3fb16839a02c9fccfdce098b6d59ef248525b -- keccak256(SolutionSubmitted(address,bytes32))
    AND contract_address = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Replace with engine_address
),
all_events AS (
  SELECT
    tx_hash,
    index,
    block_time,
    'SolutionClaimed' AS event_type,
    CAST(topic1 AS VARCHAR) AS validator,
    CAST(topic2 AS VARCHAR) AS task_id,
    NULL AS start_idx,
    NULL AS end_idx,
    NULL AS "from",
    NULL AS "to",
    NULL AS value
  FROM nova.logs
  WHERE topic0 = 0x0b76b4ae356796814d36b46f7c500bbd27b2cce1e6059a6fa2bebfd5a389b190 -- keccak256(SolutionClaimed(address,bytes32))
    AND contract_address = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Replace with engine_address
  UNION ALL
  SELECT
    tx_hash,
    index,
    block_time,
    'ContestationVoteFinish' AS event_type,
    NULL AS validator,
    CAST(topic1 AS VARCHAR) AS task_id,
    bytea2numeric(topic2) AS start_idx,
    bytea2numeric(data) AS end_idx,
    NULL AS "from",
    NULL AS "to",
    NULL AS value
  FROM nova.logs
  WHERE topic0 = 0x71d8c71303e35a39162e33a402c9897bf9848388537bac7d5e1b0d202eca4e66 -- keccak256(ContestationVoteFinish(bytes32,uint32,uint32))
    AND contract_address = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Replace with engine_address
  UNION ALL
  SELECT
    tx_hash,
    index,
    block_time,
    'Transfer' AS event_type,
    NULL AS validator,
    NULL AS task_id,
    NULL AS start_idx,
    NULL AS end_idx,
    CAST(topic1 AS VARCHAR) AS "from",
    CAST(topic2 AS VARCHAR) AS "to",
    bytea2numeric(data) AS value
  FROM nova.logs
  WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- keccak256(Transfer(address,address,uint256))
    AND contract_address = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Replace with base_token_address
    AND topic1 = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Replace with engine_address
),
events_with_group AS (
  SELECT
    *,
    SUM(CASE WHEN event_type IN ('SolutionClaimed', 'ContestationVoteFinish') THEN 1 ELSE 0 END) OVER (PARTITION BY tx_hash ORDER BY index) AS group_id
  FROM all_events
),
events_with_validator AS (
  SELECT
    e.*,
    CASE
      WHEN e.event_type = 'SolutionClaimed' THEN e.validator
      WHEN e.event_type = 'ContestationVoteFinish' THEN s.validator
      ELSE NULL
    END AS group_validator
  FROM events_with_group e
  LEFT JOIN solution_submitted s ON e.task_id = s.task_id AND e.event_type = 'ContestationVoteFinish'
),
events_with_propagated_validator AS (
  SELECT
    *,
    FIRST_VALUE(group_validator) OVER (PARTITION BY tx_hash, group_id ORDER BY index) AS validator_for_group
  FROM events_with_validator
),
events_with_group_type AS (
  SELECT
    *,
    FIRST_VALUE(event_type) OVER (PARTITION BY tx_hash, group_id ORDER BY index) AS group_type
  FROM events_with_propagated_validator
),
events_with_task_fee AS (
  SELECT
    e.*,
    tf.fee AS task_fee
  FROM events_with_group_type e
  LEFT JOIN task_fees tf ON e.task_id = tf.task_id
),
groups_with_refund AS (
  SELECT
    tx_hash,
    group_id,
    MAX(CASE WHEN event_type = 'Transfer' AND value = task_fee
             AND from_hex(substring("to", 3)) != from_hex(substring(validator_for_group, 3))
             AND from_hex(substring("to", 3)) != 0x1298F8A91B046D7FCBD5454CD3331BA6F4FEA168
             THEN 1 ELSE 0 END) AS has_refund
  FROM events_with_task_fee
  WHERE group_type = 'ContestationVoteFinish'
  GROUP BY tx_hash, group_id
),
events_with_start_idx AS (
  SELECT
    *,
    FIRST_VALUE(start_idx) OVER (PARTITION BY tx_hash, group_id ORDER BY index) AS group_start_idx
  FROM events_with_task_fee
),
transfer_with_next AS (
  SELECT
    *,
    LEAD("to") OVER (PARTITION BY tx_hash, group_id ORDER BY index) AS next_to
  FROM events_with_start_idx
  WHERE event_type = 'Transfer'
),
reward_transfers AS (
  SELECT
    t.tx_hash,
    t.index,
    t.value,
    t.validator_for_group AS validator
  FROM transfer_with_next t
  LEFT JOIN groups_with_refund r ON t.tx_hash = r.tx_hash AND t.group_id = r.group_id
  WHERE t."to" = t.validator_for_group
    AND from_hex(substring(t.next_to, 3)) = 0x1298F8A91B046D7FCBD5454CD3331BA6F4FEA168 -- Replace with treasury_address
    AND (
      (t.group_type = 'SolutionClaimed')
      OR (t.group_type = 'ContestationVoteFinish' AND t.group_start_idx = 0 AND (r.has_refund IS NULL OR r.has_refund = 0))
    )
)
SELECT
  SUM(value) AS total_rewards
FROM reward_transfers;
