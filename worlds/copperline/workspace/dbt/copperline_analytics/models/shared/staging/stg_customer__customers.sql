{{
    config(
        materialized='view'
    )
}}

{#- The trade account master.

    `customer_id` is the current format, C-######. `legacy_id` is what the
    account was called before the November 2024 re-key. Rows carrying a
    `deleted_at` are dropped: a deletion here is a privacy deletion and the row
    is meant to be gone.

    A customer reference elsewhere in the estate may be in either format. The
    crosswalk decides by the format of the id, not by the date on the row that
    carries it — docs/runbooks/customer-id-migration.md.
-#}

select
    customer_id,
    legacy_id,
    account_name,
    contact_name,
    md5(coalesce(email, ''))            as email_hash,
    md5(coalesce(phone, ''))            as phone_hash,
    address_line1,
    city,
    region_code,
    postal_code,
    country_code,
    market_code,
    tax_id,
    tier,
    status,
    billing_era,
    payment_terms_code,
    source_brand,
    created_on,
    updated_at
from {{ source('customer', 'customers') }}
where deleted_at is null
