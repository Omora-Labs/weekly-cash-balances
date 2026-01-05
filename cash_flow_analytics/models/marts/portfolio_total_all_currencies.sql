with totals_all_currencies as (
    select * from {{ ref('monthly_asset_totals_all_currencies') }}
)

select
    month,
    reporting_currency,
    sum(total_value) as porfolio_total
from totals_all_currencies
group by month, reporting_currency
order by month, reporting_currency
