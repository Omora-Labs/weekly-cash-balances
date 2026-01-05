select
    id as asset_id,
    currency_id,
    name as asset_name
from {{ source('main', 'assets') }}
