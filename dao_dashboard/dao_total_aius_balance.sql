-- https://dune.com/queries/5276442
with balances as (
    select 'ethereum_dao' as source, balance_aius from query_5274655
    union all
    select 'arbitrum_treasury' as source, total_aius as balance_aius from query_5223516
)
select
    max(case when source = 'ethereum_dao' then balance_aius end) as ethereum_dao_funds_balance,
    max(case when source = 'arbitrum_treasury' then balance_aius end) as arbitrum_treasury_balance,
    sum(balance_aius) as total_balance
from balances
