with sessions as (
    select *,
           row_number() over (partition by session_id order by session_at asc) as rn
    from {{ ref('base_sessions') }}
)
select 
    cast(session_id as varchar) as session_id,
    cast(client_id as varchar) as client_id,
    session_at,
    operating_system
from sessions 
where rn = 1 -- 必须有这一行！