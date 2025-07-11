-- https://dune.com/queries/5247542/
-- https://dune.com/queries/5247542/
WITH date_series AS (
    SELECT day
    FROM UNNEST(sequence(
        CAST('2024-02-14' AS date),
        CAST(NOW() AS date)
    )) AS t(day)
),
all_rewards AS (
    SELECT vesting_date as day, cumulative_vested as cumulative_amount, 'Vested DAO Funds' as source from query_5292842
    UNION ALL
    SELECT day, cumulative_amount, 'V1 Gysr Rewards' AS source FROM query_5245571
    UNION ALL
    SELECT day, cumulative_amount, 'V2 Gysr Rewards' AS source FROM query_5245991
    UNION ALL
    SELECT day, cumulative_withdrawn AS cumulative_amount, 'V1 Sablier Stream Withdrawals - Private Sale' AS source
    FROM query_5269610
    WHERE withdrawal_group = 'Private Sale'
    UNION ALL
    SELECT day, cumulative_withdrawn AS cumulative_amount, 'V1 Sablier Stream Withdrawals - Team' AS source
    FROM query_5269610
    WHERE withdrawal_group = 'Team'
    UNION ALL
    SELECT day, cumulative_reward AS cumulative_amount, 'VeStaking Rewards' AS source FROM query_5262526
    UNION ALL
    SELECT day, cumulative_rewards AS cumulative_amount, 'Arbius LP Staking Rewards' AS source FROM query_5264349
    UNION ALL
    SELECT day, cumulative_amount, 'Merkl LP Staking Rewards' FROM query_5264546
    UNION ALL
    SELECT day, cumulative_fee as cumulative_amount, 'LP Rewards Protocol Fees' FROM query_5268057
    UNION ALL
    SELECT day, cumulative_amount, source FROM (
        SELECT day, cumulative_treasury_rewards_tokens, cumulative_task_owner_rewards_tokens, cumulative_validator_rewards_tokens FROM query_5168325
    ) t
    CROSS JOIN UNNEST(
        ARRAY[
            ROW('Rewards Paid to Treasury - (Arbitrum One)', t.cumulative_treasury_rewards_tokens),
            ROW('Rewards Paid to Task Owners (Arbitrum One)', t.cumulative_task_owner_rewards_tokens),
            ROW('Rewards Paid to Validators (Arbitrum One)', t.cumulative_validator_rewards_tokens)
        ]
    ) AS t (source, cumulative_amount)
    UNION ALL
    SELECT day, cumulative_amount, source FROM (
        SELECT day, cumulative_treasury_rewards, cumulative_validator_rewards, cumulative_task_owner_rewards FROM dune.missingno69420.result_nova_engine_rewards -- query_5264668
    ) t
    CROSS JOIN UNNEST(
        ARRAY[
            ROW('Rewards Paid to Treasury (V2 Nova)', t.cumulative_treasury_rewards),
            ROW('Rewards Paid to Validators (V2 Nova)', t.cumulative_validator_rewards),
            ROW('Rewards Paid to Task Owners (V2 Nova)', t.cumulative_task_owner_rewards)
        ]
    ) AS t (source, cumulative_amount)
    UNION ALL
    SELECT day, cumulative_amount, source FROM (
        SELECT day, cumulative_treasury_rewards, cumulative_validator_rewards, cumulative_task_owner_rewards FROM query_5291842
    ) t
    CROSS JOIN UNNEST(
        ARRAY[
            ROW('Rewards Paid to Treasury (V1 Nova)', t.cumulative_treasury_rewards),
            ROW('Rewards Paid to Validators (V1 Nova)', t.cumulative_validator_rewards),
            ROW('Rewards Paid to Task Owners (V1 Nova)', t.cumulative_task_owner_rewards)
        ]
    ) AS t (source, cumulative_amount)
    UNION ALL
    SELECT date_trunc('day', evt_block_time) AS day, value / 1e18 AS cumulative_amount, 'Initial Dex Liquidity' AS source
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address = 0xe3DBC4F88EAa632DDF9708732E2832EEaA6688AB
      AND evt_tx_hash = 0xd2842d9e23b8452d4b43b4b112b826e23e60282ac72830eb7562fb51f60c27af
      AND evt_index = 1
),
all_sources AS (
    SELECT DISTINCT source FROM all_rewards
),
all_days_sources AS (
    SELECT d.day, s.source
    FROM date_series d
    CROSS JOIN all_sources s
),
joined AS (
    SELECT
        ads.day,
        ads.source,
        ar.cumulative_amount
    FROM all_days_sources ads
    LEFT JOIN all_rewards ar
      ON ar.day = ads.day AND ar.source = ads.source
),
filled AS (
    SELECT
        day,
        source,
        MAX(cumulative_amount) OVER (
            PARTITION BY source ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_amount
    FROM joined
)
SELECT
    day,
    source,
    COALESCE(cumulative_amount, 0) AS cumulative_amount
FROM filled
ORDER BY day DESC, source;
