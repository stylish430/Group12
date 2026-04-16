-- models/intermediate/int_sessions.sql
with sessions as (
    select *,
           row_number() over (partition by session_id order by session_at asc) as rn
    from {{ ref('base_sessions') }}
),
unique_sessions as (
    select * from sessions where rn = 1
)
select 
    -- 核心修复：强制转换为 varchar
    cast(client_id as varchar) as client_id,
    cast(session_id as varchar) as session_id,
    operating_system,
    ip_address,
    session_at
from unique_sessions