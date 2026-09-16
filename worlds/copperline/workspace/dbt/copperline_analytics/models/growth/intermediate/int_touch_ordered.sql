{{ config(materialized='view') }}

{#-
    Every marketing touch a visitor had, in order, across sessions.

    int_session_attributed answers "which campaign gets this session". This one
    answers the harder question: how many touches came before an order, over how
    many days, and which channel opened the path.

    The identity is `anonymous_id`, which survives across sessions on the same
    device and nothing else. It does not follow a person to another device and
    it does not survive a cleared browser. So the path lengths here are a floor,
    and `touch_count` should be read as "at least this many".
-#}

with touches as (

    select
        s.anonymous_id,
        s.session_id,
        s.session_date,
        s.attributed_source,
        s.attributed_medium,
        s.attributed_campaign,
        s.is_direct,
        s.converted,
        s.order_id,
        s.booked_cents,
        row_number() over (partition by s.anonymous_id order by s.session_date, s.session_id) as touch_seq
    from {{ ref('int_session_attributed') }} s
    where s.anonymous_id is not null

),

paths as (

    select
        anonymous_id,
        count(*)                                            as touch_count,
        count(*) filter (where not is_direct)               as paid_touch_count,
        min(session_date)                                   as first_touch_date,
        max(session_date)                                   as last_touch_date,
        count(*) filter (where converted)                   as converting_sessions,
        sum(booked_cents)                                   as booked_cents,
        min(session_date) filter (where converted)          as first_order_date
    from touches
    group by 1

)

select
    t.anonymous_id,
    t.session_id,
    t.session_date,
    t.touch_seq,
    t.attributed_source,
    t.attributed_medium,
    t.attributed_campaign,
    t.is_direct,
    t.converted,
    t.order_id,
    t.booked_cents,

    p.touch_count,
    p.paid_touch_count,
    p.first_touch_date,
    p.last_touch_date,
    p.converting_sessions,
    p.first_order_date,
    date_diff('day', p.first_touch_date, p.last_touch_date) as path_days,

    t.touch_seq = 1                             as is_first_touch,
    t.touch_seq = p.touch_count                 as is_last_touch,
    p.converting_sessions > 0                   as path_converted

from touches t
join paths p on p.anonymous_id = t.anonymous_id
