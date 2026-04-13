with source as (

    select *
    from {{ source('google_drive_schema', 'HR_QUITS') }}

),

cleaned as (

    select
        -- business fields
        cast(employee_id as varchar)      as employee_id,
        try_cast(quit_date as date)       as quit_date,

        -- metadata (optional but useful)
        _file                             as source_file,
        _line                             as source_line,
        try_cast(_modified as timestamp)  as modified_at,
        try_cast(_fivetran_synced as timestamp) as fivetran_synced_at

    from source

),


deduped as (

    select *
    from (

        select *,
            row_number() over (
                partition by employee_id, quit_date
                order by fivetran_synced_at desc
            ) as rn
        from cleaned

    )
    where rn = 1

),

final as (

    select *
    from deduped
    where employee_id is not null
      and quit_date is not null

)

select * from final