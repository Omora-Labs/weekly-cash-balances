with currency_pairs as (
    select * from {{ ref('monthly_exchange_rates') }} -- refers to dbt model
)

select distinct
    currency_pair_name
from currency_pairs
