-- Convert all currency totals to a single target currency

with ranked_rates as (
    select
        cp.base_currency_id,
        cp.quote_currency_id,
        er.date,
        er.value,
        date_trunc('month', er.date) as month_start,
        row_number() over (
            partition by
                cp.id,
                date_trunc('month', er.date) order by er.date desc
        ) as rn
    from exchange_rates er
    join currency_pairs cp on er.currency_pair_id = cp.id
)

select
    rr.base_currency_id,
    rr.quote_currency_id,
    bc.name || '/' || qc.name as currency_pair_name,
    rr.month_start as month,
    rr.value as last_rate
from ranked_rates rr
join currencies bc on rr.base_currency_id = bc.id
join currencies qc on rr.quote_currency_id = qc.id
where rr.rn = 1
order by month, currency_pair_name
