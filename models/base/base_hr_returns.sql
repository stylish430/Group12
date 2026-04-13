
select
    cast(order_id as varchar) as order_id,
    try_to_date(returned_at) as returned_at,

    case
        when lower(trim(is_refunded)) in ('yes', 'y', 'true', '1') then true
        when lower(trim(is_refunded)) in ('no', 'n', 'false', '0') then false
        else null
    end as is_refunded,

    _file as source_file,
    _line as source_line,
    _modified::timestamp_ntz as modified_at,
    _fivetran_synced::timestamp_ntz as fivetran_synced_at

from {{ source('google_drive_schema', 'HR_RETURNS') }}