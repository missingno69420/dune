-- https://dune.com/queries/5191539/
SELECT
    get_href(
        'https://dune.com/missingno69420/arbius-model-dashboard?model_id_t91181=0x' || model_id,
        CASE
            WHEN model_name IS NULL OR model_name = '' THEN '0x' || model_id
            ELSE model_name
        END
    ) AS dashboard_link
FROM query_5191530 -- registered models with names
