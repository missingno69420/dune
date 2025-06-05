-- https://dune.com/queries/5187635
SELECT
    COALESCE(MAX(m.model_name), to_hex(t.model)) AS model,
    COUNT(t.id) AS unsolved_tasks
FROM
    arbius_arbitrum.engine_evt_tasksubmitted t
LEFT JOIN
    arbius_arbitrum.engine_evt_solutionsubmitted s
    ON t.id = s.task
LEFT JOIN
    query_5169304 m
    ON t.model = m.model_id
WHERE
    s.evt_tx_hash IS NULL
GROUP BY
    t.model
