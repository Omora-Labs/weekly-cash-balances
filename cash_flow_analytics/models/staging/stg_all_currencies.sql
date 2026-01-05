
select distinct
    c.id as currency_id,
    c.name as currency_name
from currencies c
where exists (
    select 1 from currency_pairs cp
    where cp.quote_currency_id = c.id
)
