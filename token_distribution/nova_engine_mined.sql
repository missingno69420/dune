-- https://dune.com/queries/5247068
-- NOTE: For best performance, set your date range below to match your actual data range
WITH date_series AS (
    SELECT CAST(date_value AS DATE) AS reward_date
    FROM UNNEST(
        sequence(
            CAST('2024-02-22' AS DATE),
            CAST('2024-06-12' AS DATE),
            INTERVAL '1' DAY
        )
    ) AS t(date_value)
),
transfers AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        evt_block_time AS block_time,
        "to",
        value
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
      AND "from" = 0x3BF6050327Fa280Ee1B5F3e8Fd5EA2EfE8A6472a
      AND evt_block_time >= CAST('2024-02-22' AS TIMESTAMP)
      AND evt_block_time < CAST('2024-06-13' AS TIMESTAMP)
),
task_details AS (
    SELECT
        t.id AS task_id,
        t.fee AS task_fee,
        t.sender AS task_owner,  -- Assuming msg.sender is task_owner
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
        task AS task_id
    FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
    WHERE evt_block_time >= CAST('2024-02-22' AS TIMESTAMP)
      AND evt_block_time < CAST('2024-06-13' AS TIMESTAMP)
    UNION ALL
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
    SELECT
        tx_hash,
        log_index,
        event_type,
        task_id,
        LAG(log_index) OVER (PARTITION BY tx_hash ORDER BY log_index) AS prev_log_index,
        LEAD(log_index) OVER (PARTITION BY tx_hash ORDER BY log_index) AS next_log_index
    FROM events
),
transfer_assignments AS (
    SELECT
        t.tx_hash,
        t.log_index AS transfer_log_index,
        t.block_time,
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
),
daily_rewards AS (
    SELECT
        DATE_TRUNC('day', block_time) AS reward_date,
        recipient_type,
        SUM(reward_amount) AS daily_rewards
    FROM reward_transfers
    GROUP BY DATE_TRUNC('day', block_time), recipient_type
)
SELECT
    ds.reward_date AS day,
    COALESCE(SUM(CASE WHEN dr.recipient_type = 'treasury' THEN dr.daily_rewards ELSE 0 END),0) / 1e18 AS daily_treasury_rewards,
    COALESCE(SUM(CASE WHEN dr.recipient_type = 'validator' THEN dr.daily_rewards ELSE 0 END),0) / 1e18 AS daily_validator_rewards,
    COALESCE(SUM(CASE WHEN dr.recipient_type = 'task_owner' THEN dr.daily_rewards ELSE 0 END),0) / 1e18 AS daily_task_owner_rewards,
    COALESCE(SUM(SUM(CASE WHEN dr.recipient_type = 'treasury' THEN dr.daily_rewards ELSE 0 END)) OVER (ORDER BY ds.reward_date),0) / 1e18 AS cumulative_treasury_rewards,
    COALESCE(SUM(SUM(CASE WHEN dr.recipient_type = 'validator' THEN dr.daily_rewards ELSE 0 END)) OVER (ORDER BY ds.reward_date),0) / 1e18 AS cumulative_validator_rewards,
    COALESCE(SUM(SUM(CASE WHEN dr.recipient_type = 'task_owner' THEN dr.daily_rewards ELSE 0 END)) OVER (ORDER BY ds.reward_date),0) / 1e18 AS cumulative_task_owner_rewards,
    COALESCE(SUM(SUM(dr.daily_rewards)) OVER (ORDER BY ds.reward_date),0) / 1e18 AS cumulative_total_rewards
FROM date_series ds
LEFT JOIN daily_rewards dr ON ds.reward_date = dr.reward_date
GROUP BY ds.reward_date
ORDER BY ds.reward_date;
