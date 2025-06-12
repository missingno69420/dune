WITH events AS (
    -- Select SolutionClaimed events
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        'SolutionClaimed' AS event_type,
        task AS task_id
    FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
    WHERE evt_block_time >= CAST('2024-02-22' AS TIMESTAMP)
      AND evt_block_time < CAST('2024-06-13' AS TIMESTAMP)
    UNION ALL
    -- Select ContestationVoteFinish events
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        'ContestationVoteFinish' AS event_type,
        id AS task_id
    FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
    WHERE start_idx = 0
      AND evt_block_time >= CAST('2024-02-22' AS TIMESTAMP)
      AND evt_block_time < CAST('2024-06-13' AS TIMESTAMP)
),
events_with_bounds AS (
    -- Define bounds for transfers associated with each event
    SELECT
        tx_hash,
        log_index,
        event_type,
        task_id,
        LAG(log_index) OVER (PARTITION BY tx_hash ORDER BY log_index) AS prev_log_index,
        LEAD(log_index) OVER (PARTITION BY tx_hash ORDER BY log_index) AS next_log_index
    FROM events
),
transfers AS (
    -- Fetch ERC20 transfers from the engine contract
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        "to" AS transfer_to,
        value AS transfer_value
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
      AND "from" = 0x3BF6050327Fa280Ee1B5F3e8Fd5EA2EfE8A6472a
      AND evt_block_time >= CAST('2024-02-22' AS TIMESTAMP)
      AND evt_block_time < CAST('2024-06-13' AS TIMESTAMP)
),
task_details AS (
    -- Get task details including fee, validator, and task owner
    SELECT
        t.id AS task_id,
        t.fee AS task_fee,
        t.sender AS task_owner,
        s.addr AS validator
    FROM arbius_nova.engine_nova_obselete_evt_TaskSubmitted t
    LEFT JOIN arbius_nova.engine_nova_obselete_evt_SolutionSubmitted s
        ON t.id = s.task
),
transfer_assignments AS (
    -- Assign transfers to events based on log index bounds
    SELECT
        e.tx_hash,
        e.log_index AS event_log_index,
        e.event_type,
        e.task_id,
        t.log_index AS transfer_log_index,
        t.transfer_to,
        t.transfer_value,
        td.task_fee,
        td.validator,
        td.task_owner
    FROM events_with_bounds e
    JOIN transfers t ON e.tx_hash = t.tx_hash
    LEFT JOIN task_details td ON e.task_id = td.task_id
    WHERE
        -- For SolutionClaimed, transfers occur after the event
        (e.event_type = 'SolutionClaimed' AND t.log_index > e.log_index AND (e.next_log_index IS NULL OR t.log_index < e.next_log_index))
        OR
        -- For ContestationVoteFinish, transfers occur before the event but after the previous event
        (e.event_type = 'ContestationVoteFinish' AND t.log_index < e.log_index AND (e.prev_log_index IS NULL OR t.log_index > e.prev_log_index))
),
transfer_assignments_with_window AS (
    -- Add previous and next transfer recipients for reward pattern detection
    SELECT
        tx_hash,
        event_log_index,
        event_type,
        task_id,
        transfer_log_index,
        transfer_to,
        transfer_value,
        task_fee,
        validator,
        task_owner,
        LAG(transfer_to) OVER (PARTITION BY tx_hash ORDER BY transfer_log_index) AS prev_to,
        LEAD(transfer_to) OVER (PARTITION BY tx_hash ORDER BY transfer_log_index) AS next_to
    FROM transfer_assignments
),
reward_transfers AS (
    -- Identify reward transfers based on patterns (validator -> treasury)
    SELECT
        tx_hash,
        transfer_log_index,
        CASE
            WHEN transfer_to = validator AND next_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN 'validator_reward'
            WHEN transfer_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AND prev_to = validator THEN 'treasury_reward'
        END AS transfer_type
    FROM transfer_assignments_with_window
    WHERE
        (transfer_to = validator AND next_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168)
        OR
        (transfer_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 AND prev_to = validator)
),
fee_transfers AS (
    -- Filter out reward transfers to get fee transfers
    SELECT
        t.tx_hash,
        t.event_log_index,
        t.event_type,
        t.task_id,
        t.transfer_to,
        t.transfer_value,
        t.task_fee,
        t.validator,
        t.task_owner
    FROM transfer_assignments_with_window t
    LEFT JOIN reward_transfers r ON t.tx_hash = r.tx_hash AND t.transfer_log_index = r.transfer_log_index
    WHERE r.tx_hash IS NULL
),
fee_transfers_classified AS (
    -- Classify fee transfers as model_fee, validator_fee, or task_owner_refund
    SELECT
        tx_hash,
        event_log_index,
        event_type,
        task_id,
        transfer_to,
        transfer_value,
        task_fee,
        validator,
        task_owner,
        CASE
            WHEN transfer_to = validator THEN 'validator_fee'
            WHEN transfer_to = task_owner THEN 'task_owner_refund'
            ELSE 'model_fee'
        END AS fee_type
    FROM fee_transfers
),
event_fees AS (
    -- Aggregate fees per event, excluding task_owner_refund
    SELECT
        tx_hash,
        event_log_index,
        event_type,
        task_id,
        COALESCE(SUM(CASE WHEN fee_type = 'model_fee' THEN transfer_value ELSE 0 END), 0) AS model_fee,
        COALESCE(SUM(CASE WHEN fee_type = 'validator_fee' THEN transfer_value ELSE 0 END), 0) AS validator_fee,
        task_fee
    FROM fee_transfers_classified
    GROUP BY tx_hash, event_log_index, event_type, task_id, task_fee
),
event_fees_with_treasury AS (
    -- Calculate treasury fee as 10% of remaining fee after model fee, preventing underflow
    SELECT
        tx_hash,
        event_log_index,
        event_type,
        task_id,
        model_fee,
        validator_fee,
        COALESCE(GREATEST(task_fee - model_fee, 0) * 0.1, 0) AS treasury_fee -- Updated to prevent underflow
    FROM event_fees
    WHERE task_fee IS NOT NULL
)
-- Final output with fees in human-readable format (divided by 1e18)
SELECT
    tx_hash,
    event_type,
    task_id,
    model_fee / 1e18 AS model_fee,
    validator_fee / 1e18 AS validator_fee,
    treasury_fee / 1e18 AS treasury_fee
FROM event_fees_with_treasury
ORDER BY tx_hash, event_log_index;
