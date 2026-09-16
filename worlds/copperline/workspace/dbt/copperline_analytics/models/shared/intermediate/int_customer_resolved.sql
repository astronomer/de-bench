{{
    config(
        materialized='view'
    )
}}

{#-
    Every customer reference in the estate, resolved to one account key.

    A reference to a customer arrives in three shapes and this is the only place
    that knows all three:

    * `C-######`, the current trade account id.
    * `CUST####`, the pre-cutover id. The crosswalk maps it. Sixty legacy ids
      have no current account and stay on the legacy id — they are not errors and
      they are not dropped.
    * `NWA-#####`, a Northwave account. It has never been merged into the
      customer master. ops.merge_candidates holds the decisions somebody has
      actually made; a candidate with no `decided_by` has not been decided and is
      not applied here.

    **The crosswalk applies by the format of the id, not by the date on the row
    that carries it.** docs/runbooks/customer-id-migration.md holds that
    sentence and it is the one people get wrong: forty-five rows in the old
    format landed after the cutover date, so switching on the date puts them on
    the wrong account.

    This started as a customer-team model. It is in the shared layer because
    finance's invoice book carries the same three shapes and was resolving them
    with its own copy of the rule, which drifted. Rule 4.
-#}

with customers as (

    select
        customer_id,
        legacy_id,
        account_name,
        market_code,
        region_code,
        country_code,
        tier,
        status,
        billing_era,
        payment_terms_code,
        email_hash,
        created_on,
        updated_at
    from {{ ref('stg_customer__customers') }}

),

crosswalk as (

    select legacy_id, customer_id, is_orphan
    from {{ ref('stg_customer__id_map') }}

),

nwv as (

    select
        nwv_account_id,
        account_name,
        billing_state    as region_code,
        country_code,
        status,
        email_hash,
        opened_on
    from {{ ref('stg_customer__nwv_accounts') }}

),

decided_merges as (

    select nwv_account_id, customer_id
    from {{ ref('stg_ops__merge_candidates') }}
    where is_decided
      and customer_id is not null

),

-- One row per reference anybody can hold, current ids first.
current_ids as (

    select
        c.customer_id           as party_ref,
        'current'               as ref_shape,
        c.customer_id           as customer_key,
        'copperline'            as source_book,
        c.account_name,
        c.market_code,
        c.region_code,
        c.country_code,
        c.tier,
        c.status,
        c.billing_era,
        c.payment_terms_code,
        c.email_hash,
        c.created_on            as opened_on,
        false                   as is_orphan_reference
    from customers c

),

legacy_ids as (

    select
        x.legacy_id             as party_ref,
        'legacy'                as ref_shape,
        coalesce(x.customer_id, x.legacy_id) as customer_key,
        'copperline'            as source_book,
        c.account_name,
        c.market_code,
        c.region_code,
        c.country_code,
        c.tier,
        c.status,
        c.billing_era,
        c.payment_terms_code,
        c.email_hash,
        c.created_on            as opened_on,
        x.is_orphan             as is_orphan_reference
    from crosswalk x
    left join customers c on c.customer_id = x.customer_id

),

northwave_ids as (

    select
        n.nwv_account_id        as party_ref,
        'northwave'             as ref_shape,
        -- A decided merge folds the Northwave account onto the Copperline one.
        -- Everything else keeps its own key: an undecided candidate is not a
        -- merge, and treating it as one is how the acquired book got double
        -- counted in FY2025.
        coalesce(m.customer_id, n.nwv_account_id) as customer_key,
        'northwave'             as source_book,
        n.account_name,
        cast(null as varchar)   as market_code,
        n.region_code,
        n.country_code,
        cast(null as varchar)   as tier,
        n.status,
        cast(null as varchar)   as billing_era,
        cast(null as varchar)   as payment_terms_code,
        n.email_hash,
        n.opened_on,
        m.customer_id is null   as is_orphan_reference
    from nwv n
    left join decided_merges m on m.nwv_account_id = n.nwv_account_id

)

select * from current_ids
union all
select * from legacy_ids
union all
select * from northwave_ids
