-- models/intermediate/int_session_events_agg.sql
with page_views as (
    select 
        session_id,
        max(case when page_name = 'shopping_page' then 1 else 0 end) as has_viewed_shopping_page
    from {{ ref('base_page_views') }}
    group by 1
),
item_views as (
    select 
        session_id,
        max(case when add_to_cart_quantity > 0 then 1 else 0 end) as has_added_to_cart,
        max(1) as has_viewed_item -- 只要在 item_views 出现过即视为看过商品
    from {{ ref('base_item_views') }} -- 请确认 base 文件名是 reviews 还是 views
    group by 1
)
select 
    s.session_id,
    coalesce(pv.has_viewed_shopping_page, 0) as has_viewed_shopping_page,
    coalesce(iv.has_viewed_item, 0) as has_viewed_item,
    coalesce(iv.has_added_to_cart, 0) as has_added_to_cart
from {{ ref('int_sessions') }} s
left join page_views pv on s.session_id = pv.session_id
left join item_views iv on s.session_id = iv.session_id