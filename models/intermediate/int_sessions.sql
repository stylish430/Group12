-- models/intermediate/int_sessions.sql
with sessions as (
    select *,
           row_number() over (
               partition by session_id 
               order by try_to_timestamp(session_at) asc
           ) as rn
    from {{ ref('base_sessions') }}
),

dedup_sessions as (
    select
        session_id,
        client_id,
        try_to_timestamp(session_at) as session_at,
        operating_system
    from sessions
    where rn = 1
),

page_views as (
    select
        session_id,
        count(*) as total_page_views,
        max(case when page_name = 'shopping_page' then 1 else 0 end) as has_viewed_shopping_page
    from {{ ref('base_page_views') }}
    group by 1
),

item_events as (
    select
        session_id,
        count(*) as total_item_views,
        sum(add_to_cart_quantity) as total_added_to_cart,
        sum(remove_from_cart_quantity) as total_removed_from_cart,
        max(1) as has_viewed_item,
        max(case when add_to_cart_quantity > 0 then 1 else 0 end) as has_added_to_cart
    from {{ ref('base_item_views') }}
    group by 1
)

select
    s.session_id,
    s.client_id,
    s.session_at,
    s.operating_system,

    coalesce(p.total_page_views, 0) as total_page_views,
    coalesce(p.has_viewed_shopping_page, 0) as has_viewed_shopping_page,

    coalesce(i.total_item_views, 0) as total_item_views,
    coalesce(i.total_added_to_cart, 0) as total_added_to_cart,
    coalesce(i.total_removed_from_cart, 0) as total_removed_from_cart,

    coalesce(i.has_viewed_item, 0) as has_viewed_item,
    coalesce(i.has_added_to_cart, 0) as has_added_to_cart

from dedup_sessions s
left join page_views p using (session_id)
left join item_events i using (session_id)