{{
    config(
        materialized='table'
    )
}}

{#-
    The four selling channels.

    Written out rather than derived, because a channel that stopped selling
    still needs a row and a `select distinct` over the orders would drop it.

    `trade` is the fourth channel and several documents in this repo still say
    there are three. It is about six per cent of orders and about a third of
    revenue, it invoices on terms rather than taking a card, and any model that
    filters to the other three drops it silently. contracts/order_economics.yml
    still pins the three-value list; closing that gap is a contract amendment,
    not a model change, and docs/change-management.md is how it is done.
-#}

select * from (
    values
        ('store',       'Store',        'physical',   true,  false, 1),
        ('web',         'Web',          'digital',    true,  false, 2),
        ('marketplace', 'Marketplace',  'digital',    false, false, 3),
        ('trade',       'Trade',        'account',    false, true,  4)
) as t (
    channel_key,
    channel_name,
    channel_kind,
    is_direct_to_consumer,
    invoices_on_terms,
    display_order
)
