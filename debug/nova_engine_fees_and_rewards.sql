
WITH params AS (
    SELECT CAST('2024-02-22' AS TIMESTAMP) AS start_date,
           CAST('2024-03-01' AS TIMESTAMP) AS end_date
),
events AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        'SolutionClaimed' AS event_type,
        task AS task_id
    FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
    WHERE evt_block_time >= (SELECT start_date FROM params)
      AND evt_block_time < (SELECT end_date FROM params)
    UNION ALL
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        'ContestationVoteFinish' AS event_type,
        id AS task_id
    FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
    WHERE start_idx = 0
      AND evt_block_time >= (SELECT start_date FROM params)
      AND evt_block_time < (SELECT end_date FROM params)
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
transfers AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS log_index,
        "to" AS transfer_to,
        value AS transfer_value
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
      AND "from" = 0x3BF6050327Fa280Ee1B5F3e8Fd5EA2EfE8A6472a
      AND evt_block_time >= (SELECT start_date FROM params)
      AND evt_block_time < (SELECT end_date FROM params)
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
event_transfers AS (
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
    LEFT JOIN transfers t
        ON e.tx_hash = t.tx_hash
        AND (
            (e.event_type = 'SolutionClaimed'
             AND t.log_index > e.log_index
             AND (e.next_log_index IS NULL OR t.log_index < e.next_log_index))
            OR
            (e.event_type = 'ContestationVoteFinish'
             AND t.log_index < e.log_index
             AND (e.prev_log_index IS NULL OR t.log_index > e.prev_log_index))
        )
    LEFT JOIN task_details td
        ON e.task_id = td.task_id
),
event_fees AS (
    SELECT
        tx_hash,
        event_type,
        task_id,
        COALESCE(SUM(CASE WHEN transfer_to != validator AND transfer_to != 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN transfer_value ELSE 0 END), 0) AS model_fee,
        COALESCE(SUM(CASE WHEN transfer_to = validator THEN transfer_value ELSE 0 END), 0) AS total_validator_payment,
        COALESCE(SUM(CASE WHEN transfer_to = 0x1298f8a91b046d7fcbd5454cd3331ba6f4fea168 THEN transfer_value ELSE 0 END), 0) AS treasury_reward,
        MAX(task_fee) AS task_fee
    FROM event_transfers
    GROUP BY tx_hash, event_type, task_id
),
event_fees_with_calculations AS (
    SELECT
        tx_hash,
        event_type,
        task_id,
        model_fee,
        total_validator_payment,
        treasury_reward,
        task_fee,
        GREATEST(task_fee - model_fee, 0) AS remaining_fee,
        (GREATEST(task_fee - model_fee, 0) * 0.1) AS treasury_fee,
        (GREATEST(task_fee - model_fee, 0) * 0.9) AS validator_fee,
        GREATEST(total_validator_payment - (GREATEST(task_fee - model_fee, 0) * 0.9), 0) AS validator_reward
    FROM event_fees
)
SELECT
    tx_hash,
    event_type,
    task_id,
    model_fee / 1e18 AS model_fee,
    validator_fee / 1e18 AS validator_fee,
    validator_reward / 1e18 AS validator_reward,
    treasury_reward / 1e18 AS treasury_reward,
    treasury_fee / 1e18 AS treasury_fee
FROM event_fees_with_calculations
ORDER BY tx_hash, task_id;
