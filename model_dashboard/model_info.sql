-- https://dune.com/queries/5230591
select
    rate_percentage as solution_mineable_rate,
    COALESCE(CAST(model_fee AS double) / 1e18, 0) as model_fee_aius,
    from_hex(current_owner) as model_owner
from query_5216421 as solution_mineable_rates
join query_5230506 as model_fees
on solution_mineable_rates.model_id = model_fees.model_id
join query_5230623 as model_owners
on solution_mineable_rates.model_id = model_owners.model_id
where solution_mineable_rates.model_id = to_hex({{model_id}})
