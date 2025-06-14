-- https://dune.com/queries/5282752/
WITH date_series AS (
    SELECT day
    FROM UNNEST(sequence(
        CAST('2024-11-10' AS date),
        CAST(NOW() AS date)
    )) AS t(day)
),
daily_fees AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        SUM(CAST(modelFee - treasuryFee AS DOUBLE)) / 1e18 AS model_owner_fees,
        SUM(CAST(treasuryFee + (remainingFee - validatorFee) AS DOUBLE)) / 1e18 AS treasury_fees,
        SUM(CAST(validatorFee AS DOUBLE)) / 1e18 AS validator_fees
    FROM arbius_arbitrum.engine_evt_feespaid
    GROUP BY 1
),
cumulative_fees AS (
    SELECT
        day,
        SUM(model_owner_fees) OVER (ORDER BY day) AS cumulative_model_owner_fees,
        SUM(treasury_fees) OVER (ORDER BY day) AS cumulative_treasury_fees,
        SUM(validator_fees) OVER (ORDER BY day) AS cumulative_validator_fees
    FROM daily_fees
),
rewards_arbitrum AS (
    SELECT day, cumulative_treasury_rewards_tokens, cumulative_task_owner_rewards_tokens, cumulative_validator_rewards_tokens
    FROM query_5168325
),
vestaking_rewards AS (
    SELECT day, cumulative_reward
    FROM query_5262526
),
treasury_rewards AS (
    SELECT day, cumulative_treasury_rewards_tokens AS cumulative_amount, 'Treasury' AS recipient_group, 'Reward' AS type, 'Rewards Paid to Treasury (Arbitrum One)' AS source
    FROM rewards_arbitrum
),
task_owner_rewards AS (
    SELECT day, cumulative_task_owner_rewards_tokens AS cumulative_amount, 'Task Owners' AS recipient_group, 'Reward' AS type, 'Rewards Paid to Task Owners (Arbitrum One)' AS source
    FROM rewards_arbitrum
),
validator_rewards AS (
    SELECT day, cumulative_validator_rewards_tokens AS cumulative_amount, 'Validators' AS recipient_group, 'Reward' AS type, 'Rewards Paid to Validators (Arbitrum One)' AS source
    FROM rewards_arbitrum
),
vestaking_source AS (
    SELECT day, cumulative_reward AS cumulative_amount, 'VeStakers' AS recipient_group, 'Reward' AS type, 'VeStaking Rewards' AS source
    FROM vestaking_rewards
),
model_owner_fees AS (
    SELECT day, cumulative_model_owner_fees AS cumulative_amount, 'Model Owners' AS recipient_group, 'Fee' AS type, 'Fees Paid to Model Owners (Arbitrum One)' AS source
    FROM cumulative_fees
),
treasury_fees AS (
    SELECT day, cumulative_treasury_fees AS cumulative_amount, 'Treasury' AS recipient_group, 'Fee' AS type, 'Fees Paid to Treasury (Arbitrum One)' AS source
    FROM cumulative_fees
),
validator_fees AS (
    SELECT day, cumulative_validator_fees AS cumulative_amount, 'Validators' AS recipient_group, 'Fee' AS type, 'Fees Paid to Validators (Arbitrum One)' AS source
    FROM cumulative_fees
),
all_sources AS (
    SELECT * FROM treasury_rewards
    UNION ALL
    SELECT * FROM task_owner_rewards
    UNION ALL
    SELECT * FROM validator_rewards
    UNION ALL
    SELECT * FROM vestaking_source
    UNION ALL
    SELECT * FROM model_owner_fees
    UNION ALL
    SELECT * FROM treasury_fees
    UNION ALL
    SELECT * FROM validator_fees
),
all_days_sources AS (
    SELECT d.day, s.source, s.recipient_group, s.type
    FROM date_series d
    CROSS JOIN (SELECT DISTINCT source, recipient_group, type FROM all_sources) s
),
filled AS (
    SELECT
        ads.day,
        ads.source,
        ads.recipient_group,
        ads.type,
        MAX(ar.cumulative_amount) OVER (
            PARTITION BY ads.source
            ORDER BY ads.day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_amount
    FROM all_days_sources ads
    LEFT JOIN all_sources ar ON ads.day = ar.day AND ads.source = ar.source
),
per_group AS (
    SELECT
        day,
        recipient_group,
        SUM(COALESCE(cumulative_amount, 0)) AS total_cumulative_amount
    FROM filled
    GROUP BY day, recipient_group
)
SELECT
    day,
    recipient_group,
    total_cumulative_amount
FROM per_group
ORDER BY day DESC, recipient_group
