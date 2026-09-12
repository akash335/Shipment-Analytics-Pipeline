{{ config(materialized='view') }}

select
    shipment_id,
    customer_id,
    warehouse_id,
    carrier_id,
    shipment_date,
    expected_delivery_date,
    actual_delivery_date,
    status,
    package_weight_kg,
    shipping_cost,
    destination_region
from {{ source('public', 'shipments') }}
