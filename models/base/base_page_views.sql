with source as (

    select *
    from {{ source('web_schema', 'PAGE_VIEWS') }}

),

cleaned as (

    select
        _fivetran_id                as page_view_id,
        page_name                  as page_name,
        session_id                 as session_id,
        view_at::timestamp         as viewed_at,
        _fivetran_deleted          as is_deleted,
        _fivetran_synced           as synced_at

    from source
    where _fivetran_deleted = false   
)

select * from cleaned