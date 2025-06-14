-- https://dune.com/queries/5282947
WITH model_data AS (
    SELECT
        rate_percentage,
        COALESCE(CAST(model_fee AS double) / 1e18, 0) AS model_fee_aius,
        CONCAT('0x', current_owner) AS model_owner
    FROM query_5216421 AS solution_mineable_rates
    JOIN query_5230506 AS model_fees
        ON solution_mineable_rates.model_id = model_fees.model_id
    JOIN query_5230623 AS model_owners
        ON solution_mineable_rates.model_id = model_owners.model_id
    WHERE solution_mineable_rates.model_id = to_hex({{model_id}})
)
SELECT 'Solution Mineable Rate' AS parameter, CAST(rate_percentage AS VARCHAR) AS value FROM model_data
UNION ALL
SELECT 'Model Fee (AIUS)' AS parameter, CAST(model_fee_aius AS VARCHAR) AS value FROM model_data
UNION ALL
SELECT 'Model Owner' AS parameter, model_owner AS value FROM model_data
