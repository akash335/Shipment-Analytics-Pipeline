{{ config(materialized='view') }}

select
    customer_id,
    customer_name,
    email,
    city,
    region,
    created_at
from {{ source('public', 'customers') }}
