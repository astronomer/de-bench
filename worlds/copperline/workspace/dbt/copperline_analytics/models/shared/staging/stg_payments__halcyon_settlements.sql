{{
    config(
        materialized='view'
    )
}}

{#- Halcyon's nightly CSV, typed.

    Everything lands as text. Three casts happen here and nowhere else:

    * `amount` is a decimal string and becomes integer cents. Rule 6.
    * `txn_datetime` is a string with no zone. Halcyon stamps US Pacific, always,
      including for the European merchant accounts.
    * `currency` is missing on about a third of the rows. The feed was built
      before Copperline sold outside the United States and never learned about
      currency; it is left NULL here. Recovering it from the merchant account is
      a join, and joins do not happen in this layer — int_gl_postings_unified
      does it.
-#}

select
    txn_id,
    merchant_ref                        as order_ref,
    merchant_acct,
    upper(status)                       as status,
    {{ to_cents('amount') }}            as amount_cents,
    amount                              as amount_text,
    currency                            as currency_code,
    cast(txn_datetime as timestamp)     as txn_time_pacific,
    settled_on                          as settled_date,
    file_date,
    batch_id
from {{ source('payments', 'pay_halcyon_settlements') }}
