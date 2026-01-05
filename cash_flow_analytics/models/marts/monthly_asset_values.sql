-- Get latest asset value per month for each asset, grouped by currency

with ranked_values as (
    select
        a.id as asset_id,
        a.name as asset_name,
        a.currency_id,
        c.name as currency_name,
        av.date,
        av.value,
        date_trunc('month', av.date) as month_start,
        row_number() over (
            partition by
                a.id, date_trunc('month', av.date)
                order by av.date desc
            ) as rn
    from asset_values av
    join assets a on av.asset_id = a.id
    join currencies c on a.currency_id = c.id
)

select
    asset_id,
    asset_name,
    currency_id,
    currency_name,
    month_start as month,
    date as last_date,
    value as last_value
from ranked_values
where rn = 1
