select
    id as currency_pair_id,
    base_currency_id,
    quote_currency_id
from {{ source('main', 'currency_pairs') }}
