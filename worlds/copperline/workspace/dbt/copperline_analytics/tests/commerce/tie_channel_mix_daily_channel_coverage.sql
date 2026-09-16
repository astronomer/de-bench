{#-
    Tie: the three channels add up to the day's booked total.

    Written when there were three channels. There are four: `trade` was added
    when the trade book moved onto the order spine, it is about six per cent of
    orders and about a third of the money, and it is in the day's total and not
    in the sum of the three this test names.

    No contract states this rule either. contracts/order_economics.yml still
    pins a three-value channel list, which is the same gap seen from the other
    side, and closing both is one amendment under docs/change-management.md.
-#}

with by_day as (

    select
        ds,
        sum(booked_cents)                                                       as booked_cents,
        sum(booked_cents) filter (where channel in ('store', 'web', 'marketplace')) as three_channel_cents
    from {{ ref('channel_mix_daily') }}
    group by 1

)

select
    ds,
    booked_cents,
    three_channel_cents,
    booked_cents - three_channel_cents as variance_cents
from by_day
where booked_cents <> three_channel_cents
