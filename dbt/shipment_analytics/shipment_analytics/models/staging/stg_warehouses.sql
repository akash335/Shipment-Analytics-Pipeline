{{ config(materialized='view') }}

select
    warehouse_id,
    warehouse_name,
    region
from {{ source('public', 'warehouses') }}
