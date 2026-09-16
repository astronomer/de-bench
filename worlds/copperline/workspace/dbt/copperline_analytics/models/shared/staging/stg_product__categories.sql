{{
    config(
        materialized='view'
    )
}}

{#- The category tree.

    One root, then departments, then the tree below them. `dept_code` is carried
    on every node, so a category rolls to a department without walking the tree.
-#}

select
    category_id,
    parent_id,
    name                                as category_name,
    dept_code,
    parent_id is null                   as is_root,
    valid_from,
    coalesce(valid_to, date '9999-12-31') as valid_to,
    valid_to is null                    as is_current
from {{ source('product', 'product_categories') }}
