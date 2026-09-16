{{ config(materialized='table') }}

{#-
    The customer dimension.

    **This is a conformed dimension and the customer team builds it.** The
    stated policy is that the platform team builds conformed dims once and no
    team forks one — projects/platform/README.md rule 5. This one never finished
    moving either. `cus_dim_customer_daily` builds it, under a platform contract
    it has to satisfy, and it is on the same "to move" list dim_product is on.

    One row per account key, over both books. A Northwave account that a person
    has decided is the same customer as a Copperline account folds onto the
    Copperline key; an undecided candidate keeps its own row, because an
    undecided candidate is not a merge and treating it as one is how the
    acquired book got double counted in FY2025.

    Status is mapped to the three values contracts/customer_360.yml pins.
    `churned` upstream is `closed` here; the source has no `suspended` value and
    the contract keeps it because the CRM will send one eventually.
-#}

with candidates as (

    select *
    from {{ ref('int_customer_resolved') }}
    where ref_shape in ('current', 'northwave')

),

-- A decided merge gives the Northwave account the Copperline key, so both rows
-- carry the same customer_key. One row survives, and it is the Copperline one:
-- the merge decision says these are the same customer, and the customer master
-- is the surviving record. The Northwave id it absorbed is kept beside it.
merged_ids as (

    select
        customer_key,
        min(party_ref) filter (where ref_shape = 'northwave') as merged_nwv_account_id,
        count(*) filter (where ref_shape = 'northwave')       as merged_nwv_count
    from candidates
    group by 1

),

resolved as (

    select c.*
    from (
        select
            *,
            row_number() over (
                partition by customer_key
                order by case ref_shape when 'current' then 0 else 1 end, party_ref
            ) as survivor_seq
        from candidates
    ) c
    where c.survivor_seq = 1

),

addresses as (

    select party_ref, address_key, address_account_count, tax_id, tax_id_account_count, city, postal_code
    from {{ ref('int_address_keyed') }}

),

merges as (

    select nwv_account_id, customer_id, confidence, method, decided_by, decided_on, is_decided
    from {{ ref('stg_ops__merge_candidates') }}

),

lifecycle as (

    select party_key, first_order_date, last_order_date, orders_lifetime, orders_12m,
           net_sales_cents_lifetime, is_churned, tenure_days
    from {{ ref('int_customer_lifecycle') }}

)

select
    r.customer_key                              as customer_key,
    r.customer_key                              as customer_id,
    r.party_ref                                 as source_account_id,
    r.ref_shape                                 as source_id_shape,
    r.source_book,

    r.account_name,
    r.market_code                               as geography_key,
    coalesce(r.region_code, 'unknown')          as region_code,
    r.country_code,
    r.tier,
    r.billing_era,
    r.payment_terms_code,
    r.email_hash,
    r.opened_on,

    case r.status
        when 'active'   then 'active'
        when 'churned'  then 'closed'
        when 'closed'   then 'closed'
        when 'suspended' then 'suspended'
        else 'active'
    end                                         as status,
    r.status                                    as source_status,

    a.address_key,
    a.address_account_count,
    a.tax_id,
    a.tax_id_account_count,
    a.city,
    a.postal_code,

    coalesce(mi.merged_nwv_account_id, m.nwv_account_id) as merge_candidate_nwv_id,
    coalesce(mi.merged_nwv_count, 0) > 0        as absorbed_northwave_account,
    m.confidence                                as merge_confidence,
    m.method                                    as merge_method,
    m.decided_by                                as merge_decided_by,
    m.decided_on                                as merge_decided_on,
    coalesce(m.is_decided, false)               as merge_is_decided,
    m.nwv_account_id is not null and not coalesce(m.is_decided, false) as has_undecided_merge,

    l.first_order_date,
    l.last_order_date,
    coalesce(l.orders_lifetime, 0)              as orders_lifetime,
    coalesce(l.orders_12m, 0)                   as orders_12m,
    coalesce(l.net_sales_cents_lifetime, 0)     as net_sales_cents,
    coalesce(l.is_churned, false)               as is_churned,
    l.tenure_days,

    r.is_orphan_reference                       as is_unmerged_northwave

from resolved r
left join addresses a on a.party_ref = r.party_ref
left join merged_ids mi on mi.customer_key = r.customer_key
left join merges m on m.nwv_account_id = coalesce(mi.merged_nwv_account_id, r.party_ref)
left join lifecycle l on l.party_key = r.customer_key
