{{
    config(
        materialized='view'
    )
}}

{#- Status transitions, from two feeds that overlap.

    `oms` and `pos_replay` both write transitions for a store order, and the
    same transition can arrive twice with different timestamps. The dedupe keeps
    the earliest arrival per (order, transition, feed) and leaves the two feeds
    distinct — collapsing them is a decision this layer does not get to make.

    The feed only holds the trailing 400 days. A model that reads it and assumes
    full history is wrong for every order older than that.
-#}

with ranked as (

    select
        order_id,
        from_status,
        to_status,
        changed_at,
        changed_by,
        feed,
        row_number() over (
            partition by order_id, from_status, to_status, feed
            order by changed_at
        ) as arrival_seq
    from {{ source('sales', 'order_status_history') }}

)

select
    {{ dbt_utils.surrogate_key(['order_id', 'from_status', 'to_status', 'feed']) }} as status_change_key,
    order_id,
    from_status,
    to_status,
    changed_at,
    changed_by,
    feed
from ranked
where arrival_seq = 1
