{{ config(materialized='table') }}

select
    s.shipment_id,
    s.customer_id,
    c.customer_name,
    c.city as customer_city,
    c.region as customer_region,

    s.warehouse_id,
    w.warehouse_name,
    w.region as warehouse_region,

    s.carrier_id,
    ca.carrier_name,

    s.shipment_date,
    s.expected_delivery_date,
    s.actual_delivery_date,
    s.status,
    s.package_weight_kg,
    s.shipping_cost,
    s.destination_region,

    case
        when s.actual_delivery_date is not null
        then s.actual_delivery_date - s.expected_delivery_date
        else null
    end as delivery_delay_days,

    case
        when s.actual_delivery_date is not null
             and s.actual_delivery_date > s.expected_delivery_date
        then 1
        else 0
    end as is_delayed,

    case
        when s.status = 'Cancelled' then 1
        else 0
    end as is_cancelled

from {{ ref('stg_shipments') }} s

left join {{ ref('stg_customers') }} c
    on s.customer_id = c.customer_id

left join {{ ref('stg_warehouses') }} w
    on s.warehouse_id = w.warehouse_id

left join {{ ref('stg_carriers') }} ca
    on s.carrier_id = ca.carrier_id
