-- models/base/base_orders.sql

with source as (

    select * from {{ source('web_schema', 'ORDERS') }}

),

base as (

    select
        -- primary key
        "_fivetran_id"                                               as order_record_id,

        -- foreign keys
        order_id,
        session_id,

        -- customer info
        client_name,
        phone,
        trim(state)                                                  as state,
        shipping_address,

        -- payment info
        lower(trim(payment_method))                                  as payment_method,
        payment_info,

        -- shipping cost (strip 'USD' prefix and cast to numeric)
        try_cast(trim(replace(shipping_cost, 'USD', '')) as numeric) as shipping_cost_usd,

        -- tax
        tax_rate::numeric                                            as tax_rate,

        -- timestamps normalized to TIMESTAMP_NTZ
        order_at,
        convert_timezone('UTC', "_fivetran_synced")::timestamp_ntz  as fivetran_synced_at,

        -- soft delete flag
        "_fivetran_deleted"                                          as is_deleted

    from source

    -- exclude soft-deleted records
    where "_fivetran_deleted" = false

)

select * from base