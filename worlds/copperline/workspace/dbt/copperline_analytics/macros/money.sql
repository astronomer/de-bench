{#
    Money.

    Rule 6: every monetary column in a landed or published table is an integer
    in minor units, named *_cents, typed bigint. No float, ever. The upstream
    applications keep decimal amounts because that is what an ERP holds; the
    conversion happens at the extract boundary and nothing here undoes it.

    These macros exist so that nobody has to remember the rounding rule.
#}

{#
    A decimal or string amount to integer cents. Used only where a feed landed a
    text amount — raw.pay_halcyon_settlements is the one that does.
    half-up, never banker's rounding: docs/finance-policy.md REV-2.
#}
{% macro to_cents(expr) -%}
    cast(round(cast({{ expr }} as decimal(18, 4)) * 100, 0) as bigint)
{%- endmacro %}


{#
    Convert an amount in cents to base currency (USD) cents, using a rate in
    parts per million. Integer arithmetic throughout: multiply first, divide
    once, round half-up on the single division.

    Every fact that converts carries three columns beside the result —
    the source amount, fx_rate_ppm and fx_rate_date — so a reader can redo the
    arithmetic without going back to the rate table.
#}
{% macro to_base_cents(amount_cents, rate_ppm) -%}
    cast(
        (cast({{ amount_cents }} as decimal(38, 0)) * cast(coalesce({{ rate_ppm }}, 1000000) as decimal(38, 0))
         + 500000) / 1000000
        as bigint
    )
{%- endmacro %}


{#
    Basis points applied to an amount in cents. Same shape: multiply, divide
    once, round half-up.
#}
{% macro apply_bps(amount_cents, bps) -%}
    cast(
        (cast({{ amount_cents }} as decimal(38, 0)) * cast({{ bps }} as decimal(38, 0)) + 5000) / 10000
        as bigint
    )
{%- endmacro %}
