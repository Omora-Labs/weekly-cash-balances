select
    id as currency_id,
    name as currency_name
from {{ source('main', 'currencies') }}
