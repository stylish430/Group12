with source as (

    select *
    from {{ source('google_drive_schema', 'HR_RETURNS') }}

),
1
renamed as (

    select
        -- business columns
        cast(order_id as varchar) as order_id,
        try_cast(returned_at as date) as returned_at,

        case
            when lower(trim(is_refunded)) in ('yes', 'y', 'true', '1') then true
            when lower(trim(is_refunded)) in ('no', 'n', 'false', '0') then false
            else null
        end as is_refunded,

        -- ingestion metadata
        _file as source_file,
        _line as source_line,
        try_cast(_modified as timestamp) as modified_at,
        try_cast(_fivetran_synced as timestamp) as fivetran_synced_at

    from source

),

cleaned as (

    select
        order_id,
        returned_at,
        is_refunded,
        source_file,
        source_line,
        modified_at,
        fivetran_synced_at
    from renamed
    where order_id is not null
      and returned_at is not null

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by order_id, returned_at
                order by fivetran_synced_at desc, modified_at desc, source_line desc
            ) as rn
        from cleaned
    )
    where rn = 1

)

select
    order_id,
    returned_at,
    is_refunded,
    source_file,
    source_line,
    modified_at,
    fivetran_synced_at
from deduped