-- https://dune.com/queries/5191696/
WITH
-- Step 1: Select payouts within the lookback_period_minutes from query_5179596
payouts AS (
    SELECT
        p.task_id,
        SUM(CASE WHEN p.event_type = 'FeesPaid' THEN p.validator_fee ELSE 0 END) AS validator_fees,
        SUM(CASE WHEN p.event_type = 'RewardsPaid' THEN p.validator_reward ELSE 0 END) AS validator_rewards
    FROM query_5179596 p
    WHERE p.block_time >= NOW() - INTERVAL '{{lookback_period_minutes}}' MINUTE
    AND p.task_id IS NOT NULL
    GROUP BY p.task_id
),
-- Step 2: Identify tasks associated with these payouts
tasks AS (
    SELECT
        t.id AS task_id,
        t.model AS model_id
    FROM arbius_arbitrum.engine_evt_tasksubmitted t
    JOIN payouts p ON t.id = p.task_id
),
-- Step 3: Calculate total incentives for each task
incentives AS (
    SELECT
        taskid AS task_id,
        SUM(amount) AS total_incentives
    FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed
    WHERE taskid IN (SELECT task_id FROM tasks)
    GROUP BY taskid
),
-- Step 4: Compute profitability per task
task_profitability AS (
    SELECT
        t.task_id,
        t.model_id,
        COALESCE(p.validator_fees, 0) AS validator_fees,
        COALESCE(p.validator_rewards, 0) AS validator_rewards,
        COALESCE(i.total_incentives, 0) AS total_incentives,
        COALESCE(p.validator_fees, 0) + COALESCE(p.validator_rewards, 0) + COALESCE(i.total_incentives, 0) AS profitability
    FROM tasks t
    LEFT JOIN payouts p ON t.task_id = p.task_id
    LEFT JOIN incentives i ON t.task_id = i.task_id
),
-- Step 5: Assign ranks within each model for min and max profitability
task_ranks AS (
    SELECT
        model_id,
        task_id,
        profitability,
        validator_fees,
        validator_rewards,
        total_incentives,
        ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY profitability ASC) AS rn_min,
        ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY profitability DESC) AS rn_max
    FROM task_profitability
),
-- Step 6: Extract min profitability tasks
min_tasks AS (
    SELECT
        model_id,
        task_id AS min_task_id,
        profitability AS min_profitability,
        validator_fees AS min_validator_fees,
        validator_rewards AS min_validator_rewards,
        total_incentives AS min_total_incentives
    FROM task_ranks
    WHERE rn_min = 1
),
-- Step 7: Extract max profitability tasks
max_tasks AS (
    SELECT
        model_id,
        task_id AS max_task_id,
        profitability AS max_profitability,
        validator_fees AS max_validator_fees,
        validator_rewards AS max_validator_rewards,
        total_incentives AS max_total_incentives
    FROM task_ranks
    WHERE rn_max = 1
),
-- Step 8: Combine min and max profitability per model
model_profitability AS (
    SELECT
        min.model_id,
        min.min_task_id,
        min.min_profitability,
        min.min_validator_fees,
        min.min_validator_rewards,
        min.min_total_incentives,
        max.max_task_id,
        max.max_profitability,
        max.max_validator_fees,
        max.max_validator_rewards,
        max.max_total_incentives
    FROM min_tasks min
    JOIN max_tasks max ON min.model_id = max.model_id
),
-- Step 9: Add model names from query_5169304
model_profitability_with_names AS (
    SELECT
        mp.*,
        COALESCE(m.model_name, to_hex(mp.model_id)) AS model_name
    FROM model_profitability mp
    LEFT JOIN query_5169304 m ON mp.model_id = m.model_id
),
-- Step 10: Assign ranks across models
final_ranks AS (
    SELECT
        model_id,
        model_name,
        min_task_id,
        min_profitability,
        min_validator_fees,
        min_validator_rewards,
        min_total_incentives,
        max_task_id,
        max_profitability,
        max_validator_fees,
        max_validator_rewards,
        max_total_incentives,
        DENSE_RANK() OVER (ORDER BY min_profitability DESC) AS rank_min_profitability,
        DENSE_RANK() OVER (ORDER BY max_profitability DESC) AS rank_max_profitability
    FROM model_profitability_with_names
)
-- Step  opon11: Final output with conversions to AIUS tokens
SELECT
    model_id,
    model_name,
    rank_min_profitability,
    rank_max_profitability,
    min_task_id,
    CAST(CAST(min_validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS min_validator_fees_aius,
    CAST(CAST(min_validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS min_validator_rewards_aius,
    CAST(CAST(min_total_incentives AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS min_total_incentives_aius,
    CAST(CAST(min_profitability AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS min_profitability_aius,
    max_task_id,
    CAST(CAST(max_validator_fees AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS max_validator_fees_aius,
    CAST(CAST(max_validator_rewards AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS max_validator_rewards_aius,
    CAST(CAST(max_total_incentives AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS max_total_incentives_aius,
    CAST(CAST(max_profitability AS DECIMAL(38,0)) AS DECIMAL(38,18)) / POWER(10, 18) AS max_profitability_aius
FROM final_ranks
ORDER BY rank_max_profitability, rank_min_profitability
