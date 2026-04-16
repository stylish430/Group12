-- models/intermediate/int_sessions.sql
with sessions as (
    select *,
           -- 使用 ROW_NUMBER 仅保留每个 session_id 的第一个实例 [cite: 78, 79]
           row_number() over (partition by session_id order by session_at asc) as rn
    from {{ ref('base_sessions') }}
)
select * exclude rn from sessions where rn = 1