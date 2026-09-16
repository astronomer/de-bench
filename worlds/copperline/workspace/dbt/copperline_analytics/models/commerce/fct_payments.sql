{{ config(materialized='table') }}

{#-
    One row per order, with what each processor says about its money.

    Both processors are here side by side rather than netted. During the
    migration quarter a payment appears in both books, and that is a shadow
    quarter and not a duplicate — docs/runbooks/processor-migration.md. Netting
    them at this grain would hide the one thing the reconciliation exists to
    find, so the netting happens in marts.recon_exceptions where it can be
    reported on.
-#}

select
    p.order_id                                  as order_key,
    p.order_id,
    p.order_ref,
    p.order_date                                as date_key,
    p.order_date,
    p.channel                                   as channel_key,
    p.market_code                               as geography_key,
    p.currency_code                             as currency_key,
    p.currency_code,

    p.payment_intent_id,
    p.payment_outcome,
    p.processor,

    p.grand_total_cents                         as order_total_cents,

    p.meridian_captured_cents,
    p.meridian_refunded_cents,
    p.meridian_chargeback_cents,
    p.meridian_event_count,
    p.meridian_first_settlement_date,

    p.halcyon_settled_cents,
    p.halcyon_reversed_cents,
    p.halcyon_txn_count,
    p.halcyon_merchant_acct,
    p.halcyon_settled_date,

    p.settled_net_cents,
    p.grand_total_cents - p.settled_net_cents   as unsettled_cents,

    p.seen_by_meridian,
    p.seen_by_halcyon,
    p.is_shadow_quarter,
    p.halcyon_match_is_ambiguous,
    p.payment_match_is_ambiguous

from {{ ref('int_payment_matched') }} p
