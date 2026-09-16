{% macro nwv_money(col) %}
    round(cast({{ col }} as numeric(18,2)), 2)
{% endmacro %}


{% macro nwv_fiscal_year(date_col) %}
    case
        when extract(month from {{ date_col }}) >= 7
        then extract(year from {{ date_col }}) + 1
        else extract(year from {{ date_col }})
    end
{% endmacro %}


{# Northwave ran a July-June fiscal year. Copperline does not. Anything that
   joins across the two books has to pick one, and this macro is the Northwave
   one. #}
