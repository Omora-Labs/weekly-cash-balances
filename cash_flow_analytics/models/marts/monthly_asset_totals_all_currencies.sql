with asset_totals as (
    select * from {{ ref('monthly_asset_totals') }}
),

exchange_rates as (
    select * from {{ ref('monthly_exchange_rates') }}
),

all_currencies as (
    select * from {{ ref('stg_all_currencies') }}
)

select
    totals.month,
    totals.currency_name as original_currency,
    ac.currency_name as reporting_currency,
    totals.num_assets,
    case
        when totals.currency_name = ac.currency_name then totals.total_value
        else totals.total_value * er.last_rate
    end as total_value
from asset_totals totals
cross join all_currencies ac
left join exchange_rates er
    on totals.currency_id = er.base_currency_id
    and ac.currency_id = er.quote_currency_id
    and totals.month = er.month
