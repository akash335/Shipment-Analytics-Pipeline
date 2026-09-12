{{ config(materialized='table') }}

select
    destination_region,

    count(*) as total_shipments,

    count(*) filter (
        where status = 'Delivered'
    ) as delivered_shipments,

    count(*) filter (
        where is_delayed = 1
    ) as delayed_shipments,

    count(*) filter (
        where is_cancelled = 1
    ) as cancelled_shipments,

    round(
        avg(shipping_cost),
        2
    ) as avg_shipping_cost,

    round(
        sum(shipping_cost),
        2
    ) as total_shipping_cost,

    round(
        avg(delivery_delay_days)
        filter (
            where delivery_delay_days is not null
        ),
        2
    ) as avg_delivery_delay_days,

    round(
        100.0 * count(*) filter (
            where is_delayed = 1
        ) / nullif(
            count(*) filter (
                where status = 'Delivered'
            ),
            0
        ),
        2
    ) as delay_rate_pct

from {{ ref('int_shipments_enriched') }}

group by destination_region
order by destination_region
