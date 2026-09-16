{#-
    Tie: every account in the dimension is in the 360.

    contracts/customer_360.yml commits consumer C-8 to one row per account. An
    account that exists in the dimension and not in the 360 is invisible to the
    service desk, which is how a Northwave account spent two months with no
    record anybody could find.
-#}

select
    d.customer_key,
    d.customer_id,
    d.source_book,
    d.status
from {{ ref('dim_customer') }} d
left join {{ ref('customer_360') }} c on c.customer_id = d.customer_id
where c.customer_id is null
