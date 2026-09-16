{{ config(materialized='table') }}

{#-
    The audience file. contracts/audience_segments.yml, consumer C-6.

    Pushed out of the building by `gro_reverse_etl_ads` and
    `gro_reverse_etl_crm`, which is why the consent column is not optional and
    why the contact is a hash. A segment file that leaves with a withdrawn
    contact in it is a reportable incident, not a bug.

    There is no consent feed. Consent is derived from the email book: a
    recipient who has ever sent an `unsub` event is withdrawn, everybody else is
    granted. That is a weaker rule than a consent table would give and it is
    stated here rather than assumed, because the derivation is the thing a
    reader has to be able to argue with. `consent_source` says so on every row.

    Segments are the recency-frequency ones customer publishes, plus the two
    growth defines for itself. Nothing here invents a segment: the upstream has
    no pre-computed segment table on purpose, so a segment is always somebody's
    definition and this is growth's.
-#}

with lifecycle as (

    select *
    from {{ ref('int_customer_lifecycle') }}
    where party_kind = 'trade'

),

resolved as (

    select party_ref, customer_key, email_hash, region_code
    from {{ ref('int_customer_resolved') }}
    where ref_shape = 'current'

),

withdrawn as (

    select distinct customer_ref
    from {{ ref('stg_marketing__email_events') }}
    where event_type = 'unsub'

),

segmented as (

    select
        l.party_key                             as customer_id,
        r.email_hash                            as contact_hash,
        r.region_code,
        l.orders_12m,
        l.net_sales_cents_12m,
        l.days_since_last_order,
        l.is_churned,
        l.is_new,
        w.customer_ref is not null              as has_withdrawn,
        case
            when l.is_new                        then 'new_90d'
            when l.is_churned                    then 'lapsed'
            when l.orders_12m >= 12              then 'high_frequency'
            when l.net_sales_cents_12m >= 5000000 then 'high_value'
            when l.days_since_last_order <= 30   then 'active_30d'
            else 'general'
        end                                     as segment_id
    from lifecycle l
    join resolved r on r.customer_key = l.party_key
    left join withdrawn w on w.customer_ref = l.party_key

)

select
    {{ ds() }}                                  as ds,
    s.segment_id,
    s.customer_id,
    coalesce(s.contact_hash, md5(s.customer_id)) as contact_hash,
    case when s.has_withdrawn then 'withdrawn' else 'granted' end as consent_state,
    'derived from email unsubscribe events'     as consent_source,
    coalesce(s.region_code, 'unknown')          as region,
    s.orders_12m,
    s.net_sales_cents_12m                       as net_sales_cents,
    s.days_since_last_order
from segmented s
