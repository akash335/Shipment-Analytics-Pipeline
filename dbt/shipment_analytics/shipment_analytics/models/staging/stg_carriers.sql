{{ config(materialized='view') }}

select
    carrier_id,
    carrier_name
from {{ source('public', 'carriers') }}
