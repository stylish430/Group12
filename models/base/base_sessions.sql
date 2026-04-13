with source as (

    select *
    from {{ source('web_schema', 'SESSIONS') }}

),

renamed as (

    select
        _fivetran_id            as session_row_id,
        session_id             as session_id,
        client_id              as client_id,
        lower(os)              as os,
        ip                     as ip_address,
        cast(session_at as timestamp) as session_at,
        cast(_fivetran_synced as timestamp) as synced_at,
        cast(_fivetran_deleted as boolean) as is_deleted

    from source

)

select *
from renamed
where is_deleted = false