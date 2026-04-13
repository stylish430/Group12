-- models/staging/stg_cart_events.sql

with source as (

    select * from {{ source('web_schema', 'ITEM_VIEWS') }}

),

cleaned as (

    select
        _fivetran_id                                            as event_id,
        session_id,

        trim(item_name)                                         as item_name,
        round(cast(price_per_unit as numeric), 2)               as price_per_unit,

        greatest(add_to_cart_quantity, 0)                       as add_to_cart_quantity,
        greatest(remove_from_cart_quantity, 0)                  as remove_from_cart_quantity,

        greatest(add_to_cart_quantity, 0)
            - greatest(remove_from_cart_quantity, 0)            as net_cart_quantity,

        cast(item_view_at as timestamp)                         as item_viewed_at,
        cast(
            replace(_fivetran_synced, ' Z', '')
            as timestamp
        )                                                       as fivetran_synced_at,

        _fivetran_deleted                                       as is_deleted

    from source

    where _fivetran_deleted = false

),

deduped as (

    select *
    from cleaned
    qualify row_number() over (
        partition by event_id
        order by fivetran_synced_at desc
    ) = 1

)

select * from deduped