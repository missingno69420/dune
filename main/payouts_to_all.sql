-- https://dune.com/queries/5194864
WITH fees_paid AS (
    -- Aggregate fees from FeesPaid events
    SELECT
        date_trunc('day', p.block_time) AS date,
        t.model,
        SUM(p.model_owner_fee) AS fees_to_model_owners,
        SUM(p.treasury_total_fee) AS fees_to_treasury,
        SUM(p.validator_fee) AS fees_to_validators
    FROM query_5179596 p
    JOIN arbius_arbitrum.engine_evt_tasksubmitted t
        ON p.task_id = t.id
    WHERE p.event_type = 'FeesPaid'
    GROUP BY date_trunc('day', p.block_time), t.model
),
rewards_paid AS (
    -- Aggregate rewards from RewardsPaid events
    SELECT
        date_trunc('day', p.block_time) AS date,
        t.model,
        SUM(p.treasury_reward) AS rewards_to_treasury,
        SUM(p.task_owner_reward) AS rewards_to_task_owners,
        SUM(p.validator_reward) AS rewards_to_validators
    FROM query_5179596 p
    JOIN arbius_arbitrum.engine_evt_tasksubmitted t
        ON p.task_id = t.id
    WHERE p.event_type = 'RewardsPaid'
    GROUP BY date_trunc('day', p.block_time), t.model
),
incentives_paid AS (
    -- Aggregate incentives from IncentiveClaimed events
    SELECT
        date_trunc('day', i.evt_block_time) AS date,
        t.model,
        SUM(i.amount) AS incentives_to_validators
    FROM arbius_arbitrum.arbiusrouterv1_evt_incentiveclaimed i
    JOIN arbius_arbitrum.engine_evt_tasksubmitted t
        ON i.taskid = t.id
    GROUP BY date_trunc('day', i.evt_block_time), t.model
),
all_dates_models AS (
    -- Base table with all unique date-model combinations
    SELECT DISTINCT
        date,
        model
    FROM (
        SELECT date, model FROM fees_paid
        UNION
        SELECT date, model FROM rewards_paid
        UNION
        SELECT date, model FROM incentives_paid
    ) AS combined
)
SELECT
    d.date,
    COALESCE(m.model_name, to_hex(d.model)) AS model,  -- Display model name if available, otherwise hex model ID
    -- Fees Paid (in AIUS)
    COALESCE(CAST(f.fees_to_model_owners AS double) / 1e18, 0) AS fees_to_model_owners,
    COALESCE(CAST(f.fees_to_treasury AS double) / 1e18, 0) AS fees_to_treasury,
    COALESCE(CAST(f.fees_to_validators AS double) / 1e18, 0) AS fees_to_validators,
    -- Rewards Paid (in AIUS)
    COALESCE(CAST(r.rewards_to_treasury AS double) / 1e18, 0) AS rewards_to_treasury,
    COALESCE(CAST(r.rewards_to_task_owners AS double) / 1e18, 0) AS rewards_to_task_owners,
    COALESCE(CAST(r.rewards_to_validators AS double) / 1e18, 0) AS rewards_to_validators,
    -- Incentives Paid (in AIUS)
    COALESCE(CAST(i.incentives_to_validators AS double) / 1e18, 0) AS incentives_to_validators
FROM all_dates_models d
LEFT JOIN query_5169304 m ON d.model = m.model_id  -- Join to get model names
LEFT JOIN fees_paid f ON d.date = f.date AND d.model = f.model
LEFT JOIN rewards_paid r ON d.date = r.date AND d.model = r.model
LEFT JOIN incentives_paid i ON d.date = i.date AND d.model = i.model
ORDER BY d.date, d.model;
