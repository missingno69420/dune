-- https://dune.com/queries/5247542/
WITH date_series AS (
    SELECT day
    FROM UNNEST(sequence(
        CAST('2024-02-14' AS date),
        CAST(NOW() AS date)
    )) AS t(day)
),
all_rewards AS (
    SELECT day, cumulative_amount, 'V1 Gysr Rewards' AS source FROM query_5245571
    UNION ALL
    SELECT day, cumulative_amount, 'V2 Gysr Rewards' AS source FROM query_5245991
    UNION ALL
    SELECT day, cumulative_withdrawn AS cumulative_amount, 'V1 Sablier Stream Withdrawals' AS source FROM query_5261742
    UNION ALL
    SELECT day, cumulative_reward AS cumulative_amount, 'VeStaking Rewards' AS source FROM query_5262526
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
        SELECT day, cumulative_treasury_rewards, cumulative_validator_rewards, cumulative_task_owner_rewards FROM query_5256172
    ) t
    CROSS JOIN UNNEST(
        ARRAY[
            ROW('Rewards Paid to Treasury (Nova)', t.cumulative_treasury_rewards),
            ROW('Rewards Paid to Validators (Nova)', t.cumulative_validator_rewards),
            ROW('Rewards Paid to Task Owners (Nova)', t.cumulative_task_owner_rewards)
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
final_rewards AS (
    SELECT
        d.day,
        s.source,
        MAX(r.cumulative_amount) OVER (
            PARTITION BY s.source ORDER BY d.day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_amount
    FROM date_series d
    CROSS JOIN all_sources s
    LEFT JOIN all_rewards r ON r.source = s.source AND r.day = d.day
)
SELECT
    day,
    source,
    COALESCE(cumulative_amount, 0) AS cumulative_amount
FROM final_rewards
ORDER BY day DESC, source;
