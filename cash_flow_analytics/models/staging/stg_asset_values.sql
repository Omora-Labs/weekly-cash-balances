select
    id as value_id,
    asset_id,
    value,
    date
from {{ source('main', 'asset_values') }}
