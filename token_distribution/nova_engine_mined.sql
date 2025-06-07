-- https://dune.com/queries/5247068
WITH date_series AS (
    SELECT
        CAST(date AS DATE) AS reward_date
    FROM (
        SELECT
            sequence(
                (SELECT CAST(MIN(DATE_TRUNC('day', evt_block_time)) AS date)
                 FROM (
                     SELECT evt_block_time FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
                     UNION
                     SELECT evt_block_time FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
                 ) AS reward_events
                ),
                (SELECT CAST(MAX(DATE_TRUNC('day', evt_block_time)) AS date)
                 FROM (
                     SELECT evt_block_time FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
                     UNION
                     SELECT evt_block_time FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
                 ) AS reward_events
                ),
                interval '1' day
            ) AS date_array
    ) AS t
    CROSS JOIN UNNEST(date_array) AS t(date)
),
transfer_events AS (
    SELECT
        evt_tx_hash AS tx_hash,
        evt_index AS index,
        evt_block_time AS block_time,
        "from",
        "to",
        value
    FROM erc20_nova.evt_transfer
    WHERE contract_address = 0x8afe4055ebc86bd2afb3940c0095c9aca511d852
      AND "from" = 0x3BF6050327Fa280Ee1B5F3e8Fd5EA2EfE8A6472a
),
task_details AS (
    SELECT
        t.id AS task_id,
        t.fee AS task_fee,
        s.addr AS validator
    FROM arbius_nova.engine_nova_obselete_evt_TaskSubmitted t
    LEFT JOIN arbius_nova.engine_nova_obselete_evt_SolutionSubmitted s ON t.id = s.task
),
all_events AS (
    SELECT 'SolutionClaimed' AS event_type, evt_tx_hash AS tx_hash, evt_index AS index, evt_block_time AS block_time, addr AS validator, task AS task_id, NULL AS "from", NULL AS "to", NULL AS value
    FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
    UNION ALL
    SELECT 'ContestationVoteFinish' AS event_type, evt_tx_hash AS tx_hash, evt_index AS index, evt_block_time AS block_time, NULL AS validator, id AS task_id, NULL AS "from", NULL AS "to", NULL AS value
    FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
    WHERE start_idx = 0
    UNION ALL
    SELECT 'Transfer' AS event_type, tx_hash, index, block_time, NULL AS validator, NULL AS task_id, "from", "to", value
    FROM transfer_events
),
events_with_group AS (
    SELECT
        *,
        SUM(CASE WHEN event_type IN ('SolutionClaimed', 'ContestationVoteFinish') THEN 1 ELSE 0 END) OVER (PARTITION BY tx_hash ORDER BY index) AS group_id
    FROM all_events
),
group_info AS (
    SELECT
        tx_hash,
        group_id,
        MAX(CASE WHEN event_type = 'SolutionClaimed' THEN validator END) AS solution_validator,
        MAX(CASE WHEN event_type = 'ContestationVoteFinish' THEN task_id END) AS task_id,
        MAX(CASE WHEN event_type = 'SolutionClaimed' THEN 1 ELSE 0 END) AS is_solution_claimed,
        MAX(CASE WHEN event_type = 'ContestationVoteFinish' THEN 1 ELSE 0 END) AS is_contestation_vote_finish
    FROM events_with_group
    WHERE event_type IN ('SolutionClaimed', 'ContestationVoteFinish')
    GROUP BY tx_hash, group_id
),
transfer_groups AS (
    SELECT
        e.tx_hash,
        e.group_id,
        e.index,
        e.block_time AS block_time,  -- Added to resolve the error
        e."from",
        e."to",
        e.value,
        LEAD(e."to") OVER (PARTITION BY e.tx_hash, e.group_id ORDER BY e.index) AS next_to,
        COALESCE(g.solution_validator, td.validator) AS validator,
        td.task_fee,
        g.is_solution_claimed,
        g.is_contestation_vote_finish
    FROM events_with_group e
    JOIN group_info g ON e.tx_hash = g.tx_hash AND e.group_id = g.group_id
    LEFT JOIN task_details td ON g.task_id = td.task_id
    WHERE e.event_type = 'Transfer'
),
refund_check AS (
    SELECT
        tx_hash,
        group_id,
        MAX(CASE WHEN value = task_fee AND "to" != validator AND "to" != 0x1298F8A91B046d7fCBd5454cd3331Ba6f4feA168 THEN 1 ELSE 0 END) AS has_refund
    FROM transfer_groups
    WHERE is_contestation_vote_finish = 1
    GROUP BY tx_hash, group_id
),
reward_transfers AS (
    SELECT
        t.tx_hash,
        t.index,
        t.block_time,
        t.value
    FROM transfer_groups t
    LEFT JOIN refund_check r ON t.tx_hash = r.tx_hash AND t.group_id = r.group_id
    WHERE (t.is_solution_claimed = 1 OR (t.is_contestation_vote_finish = 1 AND (r.has_refund = 0 OR r.has_refund IS NULL)))
      AND t."to" = t.validator
      AND t.next_to = 0x1298F8A91B046d7fCBd5454cd3331Ba6f4feA168
),
daily_rewards AS (
    SELECT
        DATE_TRUNC('day', block_time) AS reward_date,
        SUM(value) AS daily_rewards
    FROM reward_transfers
    GROUP BY DATE_TRUNC('day', block_time)
)
--SELECT COUNT(*) FROM transfer_events;
SELECT
    ds.reward_date AS day,
    CAST(COALESCE(dr.daily_rewards, 0) AS DOUBLE) / 1e18 AS daily_rewards,
    CAST(SUM(COALESCE(dr.daily_rewards, 0)) OVER (ORDER BY ds.reward_date) AS DOUBLE) / 1e18 AS cumulative_amount
FROM date_series ds
LEFT JOIN daily_rewards dr ON ds.reward_date = dr.reward_date
ORDER BY ds.reward_date;
