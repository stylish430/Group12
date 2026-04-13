-- models/staging/stg_orders.sql

with source as (

    select * from {{ source('web_schema', 'ORDERS') }}

),

cleaned as (

    select
        _fivetran_id                                                as order_record_id,
        order_id,
        session_id,

        client_name,
        phone,

        trim(state)                                                 as state,
        shipping_address,

        lower(trim(payment_method))                                 as payment_method,
        payment_info,

        cast(
            trim(replace(shipping_cost, 'USD', ''))
            as numeric
        )                                                           as shipping_cost_usd,

        tax_rate,

        cast(order_at as timestamp)                                 as order_at,
        cast(
            replace(_fivetran_synced, ' Z', '')
            as timestamp
        )                                                           as fivetran_synced_at,

        _fivetran_deleted                                           as is_deleted

    from source

    where _fivetran_deleted = false

),

deduped as (

    select *
    from cleaned
    qualify row_number() over (
        partition by order_id
        order by fivetran_synced_at desc
    ) = 1

)

select * from deduped