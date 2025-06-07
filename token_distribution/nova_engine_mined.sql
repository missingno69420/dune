-- https://dune.com/queries/5247068
-- rough query that assumes every transfer in a transaction with a solution claimed or contestation vote finish event is a reward transfer
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
        tx_hash,
        index,
        block_time,
        topic1 AS "from",  -- Sender address (VARBINARY, padded 32 bytes)
        topic2 AS "to",    -- Receiver address (VARBINARY, padded 32 bytes)
        varbinary_to_uint256(data) AS value  -- Transfer amount (decoded from data)
    FROM nova.logs
    WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef  -- Transfer event signature
      AND contract_address = 0x8AFE4055Ebc86Bd2AFB3940c0095C9aca511d852  -- ERC20 token contract address
),
reward_txs AS (
    -- Transactions from SolutionClaimed events
    SELECT evt_tx_hash AS tx_hash
    FROM arbius_nova.engine_nova_obselete_evt_SolutionClaimed
    UNION
    -- Transactions from ContestationVoteFinish events
    SELECT evt_tx_hash AS tx_hash
    FROM arbius_nova.engine_nova_obselete_evt_ContestationVoteFinish
),
transfers AS (
    SELECT
        t.tx_hash,
        t.block_time,
        t.value
    FROM transfer_events t
    INNER JOIN reward_txs rt ON t.tx_hash = rt.tx_hash
    WHERE t."from" = 0x0000000000000000000000003bf6050327fa280ee1b5f3e8fd5ea2efe8a6472a  -- Engine address
),
daily_rewards AS (
    SELECT
        DATE_TRUNC('day', block_time) AS reward_date,
        SUM(value) AS daily_rewards
    FROM transfers
    GROUP BY DATE_TRUNC('day', block_time)
)
SELECT
    ds.reward_date as day,
    CAST(COALESCE(dr.daily_rewards, 0) AS DOUBLE) / 1e18 AS daily_rewards,
    CAST(SUM(COALESCE(dr.daily_rewards, 0)) OVER (ORDER BY ds.reward_date) AS DOUBLE) / 1e18 AS cumulative_amount
FROM date_series ds
LEFT JOIN daily_rewards dr ON ds.reward_date = dr.reward_date
ORDER BY ds.reward_date;
