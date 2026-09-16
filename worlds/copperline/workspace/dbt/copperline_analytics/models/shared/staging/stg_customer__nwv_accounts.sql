{{
    config(
        materialized='view'
    )
}}

{#- The Northwave account book.

    Landed once at the acquisition and frozen. It has never been merged into the
    customer master; ops.merge_candidates holds every merge decision anyone has
    made about it and is the source of record for them.
-#}

select
    nwv_account_id,
    account_name,
    primary_contact,
    md5(coalesce(contact_email, ''))    as email_hash,
    md5(coalesce(phone, ''))            as phone_hash,
    billing_city,
    billing_state,
    country                             as country_code,
    tax_id,
    status,
    owner,
    legacy_crm_id,
    opened_on
from {{ source('customer', 'nwv_accounts') }}
