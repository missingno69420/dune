WITH target_tx AS (
    SELECT 0x8856f9e28fbe40e708b5b4178be4213c6ccdcb5b9d2c079a3e4e2a0c07667517 AS tx_hash  -- Replace with the actual block number
),
transfers AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        "to",
        value
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
      AND "from" = 0x3BF6050327Fa280Ee1B5F3e8Fd5EA2EfE8A6472a
      AND evt_tx_hash = (SELECT tx_hash FROM target_tx)
),
task_details AS (
    SELECT
        t.id AS task_id,
        t.fee AS task_fee,
        t.sender AS task_owner,
        s.addr AS validator
    FROM arbius_nova.engine_nova_obselete_evt_TaskSubmitted t
    LEFT JOIN arbius_nova.engine_nova_obselete_evt_SolutionSubmitted s
        ON t.id = s.task
),
events AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        'SolutionClaimed' AS event_type,
        task AS task_id,
        evt_block_number AS block_number
    FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
    WHERE evt_tx_hash = (SELECT tx_hash FROM target_tx)
    UNION ALL
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        'ContestationVoteFinish' AS event_type,
        id AS task_id,
        evt_block_number AS block_number
    FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
    WHERE start_idx = 0
      AND evt_tx_hash = (SELECT tx_hash FROM target_tx)
),
events_with_bounds AS (
    SELECT
        tx_hash,
        log_index,
        event_type,
        task_id,
        block_number,
        LAG(log_index) OVER (PARTITION BY tx_hash ORDER BY log_index) AS prev_log_index,
        LEAD(log_index) OVER (PARTITION BY tx_hash ORDER BY log_index) AS next_log_index
    FROM events
),
transfer_assignments AS (
    SELECT
        t.tx_hash,
        t.log_index AS transfer_log_index,
        t.block_time,
        e.block_number,
        t."to" AS transfer_to,
        t.value AS transfer_value,
        e.event_type,
        e.task_id,
        e.log_index AS event_log_index,
        e.prev_log_index,
        e.next_log_index,
        td.task_owner,
        td.validator,
        td.task_fee
    FROM transfers t
    JOIN events_with_bounds e ON t.tx_hash = e.tx_hash
    LEFT JOIN task_details td ON e.task_id = td.task_id
    WHERE
        (e.event_type = 'SolutionClaimed'
            AND t.log_index > e.log_index
            AND (e.next_log_index IS NULL OR t.log_index < e.next_log_index))
        OR
        (e.event_type = 'ContestationVoteFinish'
            AND t.log_index < e.log_index
            AND (e.prev_log_index IS NULL OR t.log_index > e.prev_log_index))
),
transfer_assignments_with_window AS (
    SELECT
        tx_hash,
        transfer_log_index,
        block_time,
        block_number,
        transfer_to,
        transfer_value,
        event_type,
        task_id,
        event_log_index,
        task_owner,
        validator,
        task_fee,
        LAG(transfer_to) OVER (PARTITION BY tx_hash ORDER BY transfer_log_index) AS prev_to,
        LEAD(transfer_to) OVER (PARTITION BY tx_hash ORDER BY transfer_log_index) AS next_to
    FROM transfer_assignments
),
refund_flags AS (
    SELECT
        tx_hash,
        event_log_index,
        MAX(CASE WHEN transfer_to = task_owner AND transfer_value = task_fee THEN 1 ELSE 0 END) AS has_refund
    FROM transfer_assignments_with_window
    WHERE event_type = 'ContestationVoteFinish'
    GROUP BY tx_hash, event_log_index
),
groups_to_consider AS (
    SELECT tx_hash, log_index, event_type
    FROM events_with_bounds
    WHERE event_type = 'SolutionClaimed'
    UNION ALL
    SELECT e.tx_hash, e.log_index, e.event_type
    FROM events_with_bounds e
    LEFT JOIN refund_flags r ON e.tx_hash = r.tx_hash AND e.log_index = r.event_log_index
    WHERE e.event_type = 'ContestationVoteFinish'
      AND (r.has_refund = 0 OR r.has_refund IS NULL)
),
reward_transfers AS (
    SELECT
        t.tx_hash,
        t.transfer_log_index,
        t.block_time,
        t.block_number,
        CASE
            WHEN t.transfer_to = t.validator
                 AND t.next_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168
                THEN 'validator'
            WHEN t.transfer_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168
                 AND t.prev_to = t.validator
                THEN 'treasury'
            WHEN t.transfer_to = t.task_owner
                 AND t.prev_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168
                THEN 'task_owner'
        END AS recipient_type,
        t.transfer_value AS reward_amount
    FROM transfer_assignments_with_window t
    JOIN groups_to_consider g ON t.tx_hash = g.tx_hash AND t.event_log_index = g.log_index
    WHERE
        (t.transfer_to = t.validator
            AND t.next_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
        OR
        (t.transfer_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168
            AND t.prev_to = t.validator)
        OR
        (t.transfer_to = t.task_owner
            AND t.prev_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
)
SELECT
    (SELECT tx_hash FROM target_tx) AS tx_tash,
    COALESCE(SUM(CASE WHEN recipient_type = 'treasury' THEN reward_amount ELSE 0 END), 0) / 1e18 AS treasury_rewards,
    COALESCE(SUM(CASE WHEN recipient_type = 'validator' THEN reward_amount ELSE 0 END), 0) / 1e18 AS validator_rewards,
    COALESCE(SUM(CASE WHEN recipient_type = 'task_owner' THEN reward_amount ELSE 0 END), 0) / 1e18 AS task_owner_rewards,
    COALESCE(SUM(reward_amount), 0) / 1e18 AS total_rewards
FROM reward_transfers;
