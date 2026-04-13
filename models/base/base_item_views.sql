-- models/base/base_item_views.sql

with source as (

    select * from {{ source('web_schema', 'ITEM_VIEWS') }}

),

base as (

    select
        -- primary key
        "_fivetran_id"                                               as event_id,

        -- foreign key
        session_id,

        -- item info
        trim(item_name)                                              as item_name,
        round(price_per_unit::numeric, 2)                           as price_per_unit,

        -- cart quantities (coalesce null to 0)
        coalesce(add_to_cart_quantity, 0)                           as add_to_cart_quantity,
        coalesce(remove_from_cart_quantity, 0)                      as remove_from_cart_quantity,

        -- timestamps normalized to TIMESTAMP_NTZ
        item_view_at,
        convert_timezone('UTC', "_fivetran_synced")::timestamp_ntz  as fivetran_synced_at,

        -- soft delete flag
        "_fivetran_deleted"                                          as is_deleted

    from source

    -- exclude soft-deleted records
    where "_fivetran_deleted" = false

)

select * from base