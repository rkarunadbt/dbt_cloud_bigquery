with raw_orders as (
    select * from {{ source('bigquery_source', 'orders') }}
),

staged as (
    select
        order_id,
        customer_id,
        order_timestamp,
        -- Standardizing naming conventions
        total_amount as order_total_usd,
        upper(status) as order_status,
        region as sales_region,
        -- Adding a load metadata column
        current_timestamp() as loaded_at
    from raw_orders
)

select * from staged