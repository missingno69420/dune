-- https://dune.com/queries/5247068
WITH task_fees AS (
  SELECT
    id AS task_id,
    fee
  FROM arbius_nova.engine_nova_obselete_evt_TaskSubmitted
),
solution_submitted AS (
  SELECT
    task AS task_id,
    validator
  FROM arbius_nova.engine_nova_obselete_evt_SolutionSubmitted
),
transfer_events AS (
  SELECT
    tx_hash,
    index,
    block_time,
    'Transfer' AS event_type,
    topic1 AS "from",
    topic2 AS "to",
    varbinary_to_uint256(data) AS value
  FROM nova.logs
  WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer event signature
    AND contract_address = 0x8AFE4055Ebc86Bd2AFB3940c0095C9aca511d852 -- Replace with base_token_address
    AND topic1 = 0x3bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a -- Engine address
),
all_events AS (
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'SolutionClaimed' AS event_type,
    validator,
    task AS task_id,
    NULL AS start_idx,
    NULL AS end_idx,
    NULL AS "from",
    NULL AS "to",
    NULL AS value
  FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
  UNION ALL
  SELECT
    evt_tx_hash AS tx_hash,
    evt_index AS index,
    evt_block_time AS block_time,
    'ContestationVoteFinish' AS event_type,
    NULL AS validator,
    id AS task_id,
    start_idx,
    end_idx,
    NULL AS "from",
    NULL AS "to",
    NULL AS value
  FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
  UNION ALL
  SELECT
    tx_hash,
    index,
    block_time,
    event_type,
    NULL AS validator,
    NULL AS task_id,
    NULL AS start_idx,
    NULL AS end_idx,
    "from",
    "to",
    value
  FROM transfer_events
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
    MAX(CASE WHEN event_type = 'Transfer' AND value = task_fee AND "to" != validator_for_group AND "to" != 0x1298F8A91B046d7fCBd5454cd3331Ba6f4feA168 THEN 1 ELSE 0 END) AS has_refund
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
    AND t.next_to = 0x1298F8A91B046d7fCBd5454cd3331Ba6f4feA168  -- Treasury address
    AND (
      (t.group_type = 'SolutionClaimed')
      OR (t.group_type = 'ContestationVoteFinish' AND t.group_start_idx = 0 AND (r.has_refund IS NULL OR r.has_refund = 0))
    )
)
SELECT
  SUM(value) AS total_rewards
FROM reward_transfers;
