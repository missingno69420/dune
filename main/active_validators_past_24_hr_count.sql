-- https://dune.com/queries/5203291/
-- Validators automatically vote nay in contestation votes on their own solutions.
-- We'll count their vote in this query as activity for the sake of writing a simple query.
SELECT COUNT(*) AS active_validator_count
FROM (
    SELECT DISTINCT addr
    FROM arbius_arbitrum.v2_enginev5_1_evt_solutionsubmitted
    WHERE evt_block_time >= NOW() - INTERVAL '24' hour
    UNION
    SELECT DISTINCT addr
    FROM arbius_arbitrum.v2_enginev5_1_evt_contestationvote
    WHERE evt_block_time >= NOW() - INTERVAL '24' hour
) AS active_validators;
