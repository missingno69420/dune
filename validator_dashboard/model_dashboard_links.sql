-- https://dune.com/queries/5191539/
SELECT
    get_href(
        'https://dune.com/arbius_dashboards/model?model_id_tee4a4=0x' || model_id,
        CASE
            WHEN model_name IS NULL OR model_name = '' THEN '0x' || model_id
            ELSE model_name
        END
    ) AS dashboard_link,
    '0x' || model_id AS model_id
FROM query_5191530
