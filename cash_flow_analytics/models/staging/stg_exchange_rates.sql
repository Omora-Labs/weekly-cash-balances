select
    currency_pair_id,
    value as rate,
    date
from {{ source('main', 'exchange_rates') }}
