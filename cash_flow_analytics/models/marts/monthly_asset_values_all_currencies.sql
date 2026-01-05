with asset_values as (
    select * from {{ ref('monthly_asset_values') }}
),

exchange_rates as (
    select * from {{ ref('monthly_exchange_rates') }}
),

all_currencies as (
    select * from {{ ref('stg_all_currencies') }}
)

select
    av.month,
    av.asset_name,
    av.currency_name as original_currency,
    ac.currency_name as reporting_currency,
    case
        when av.currency_name = ac.currency_name then av.last_value
        else av.last_value * er.last_rate
    end as value
from asset_values av
cross join all_currencies ac
left join exchange_rates er
    on av.currency_id = er.base_currency_id
    and ac.currency_id = er.quote_currency_id
    and av.month = er.month
