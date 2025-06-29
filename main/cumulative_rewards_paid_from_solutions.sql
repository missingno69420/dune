-- https://dune.com/queries/5168325/
WITH daily_totals AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        SUM(treasuryReward) AS daily_treasury,
        SUM(taskOwnerReward) AS daily_task_owner,
        SUM(validatorReward) AS daily_validator,
        SUM(totalRewards) AS daily_veaius
    FROM arbius_arbitrum.engine_evt_rewardspaid
    GROUP BY date_trunc('day', evt_block_time)
),
cumulative_sums AS (
    SELECT
        day,
        SUM(daily_treasury) OVER (ORDER BY day) AS cumulative_treasury,
        SUM(daily_task_owner) OVER (ORDER BY day) AS cumulative_task_owner,
        SUM(daily_validator) OVER (ORDER BY day) AS cumulative_validator,
        SUM(daily_veaius) OVER (ORDER BY day) AS cumulative_veaius
    FROM daily_totals
)
SELECT
    day,
    COALESCE(cumulative_treasury, 0) / 1e18 AS cumulative_treasury_rewards_tokens,
    COALESCE(cumulative_task_owner, 0) / 1e18 AS cumulative_task_owner_rewards_tokens,
    COALESCE(cumulative_validator, 0) / 1e18 AS cumulative_validator_rewards_tokens,
    COALESCE(cumulative_veaius, 0) / 1e18 AS cumulative_veaius_rewards_tokens
FROM cumulative_sums
ORDER BY day;
