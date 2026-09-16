{{ config(materialized='view') }}

{#-
    One address key per account, so that two accounts at the same place can be
    seen to be at the same place.

    The key is the normalised postal address: upper case, punctuation removed,
    whitespace collapsed. It is deliberately crude. It finds the same building
    written two ways and it does not find a moved account, and both of those
    are stated rather than implied.

    **An address match is not a merge.** ops.merge_candidates is the source of
    record for whether two accounts are one customer, and a decision there needs
    a person's name on it. This model produces a candidate, nothing more, and
    `address_account_count` is how many accounts share the key.
-#}

with copperline as (

    select
        customer_id                             as party_ref,
        'current'                               as ref_shape,
        account_name,
        address_line1,
        city,
        region_code,
        postal_code,
        country_code,
        tax_id
    from {{ ref('stg_customer__customers') }}

),

northwave as (

    select
        nwv_account_id                          as party_ref,
        'northwave'                             as ref_shape,
        account_name,
        cast(null as varchar)                   as address_line1,
        billing_city                            as city,
        billing_state                           as region_code,
        cast(null as varchar)                   as postal_code,
        country_code,
        tax_id
    from {{ ref('stg_customer__nwv_accounts') }}

),

both_books as (

    select * from copperline
    union all
    select * from northwave

),

keyed as (

    select
        *,
        upper(regexp_replace(
            trim(coalesce(address_line1, '') || ' ' || coalesce(city, '') || ' '
                 || coalesce(postal_code, '') || ' ' || coalesce(country_code, '')),
            '[^A-Za-z0-9 ]', '', 'g'
        ))                                      as address_text
    from both_books

)

select
    party_ref,
    ref_shape,
    account_name,
    address_line1,
    city,
    region_code,
    postal_code,
    country_code,
    tax_id,

    regexp_replace(address_text, ' +', ' ', 'g')                as address_normalized,
    md5(regexp_replace(address_text, ' +', ' ', 'g'))           as address_key,

    count(*) over (partition by md5(regexp_replace(address_text, ' +', ' ', 'g'))) as address_account_count,
    case when tax_id is not null then count(*) over (partition by tax_id) end     as tax_id_account_count,

    trim(address_text) = ''                     as address_is_empty

from keyed
