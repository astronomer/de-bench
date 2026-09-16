{{
    config(
        materialized='view'
    )
}}

{#- The lane master: origin DC to destination region, with both timezones.

    Both timezone names are on the row because a transit time in hours is wrong
    if either end is read in the other's zone, and the lanes cross eight zones.
-#}

select
    lane_id,
    origin_dc,
    dest_region,
    dest_country,
    region_code,
    origin_tz_name,
    dest_tz_name,
    active_from,
    coalesce(active_to, date '9999-12-31')  as active_to,
    active_to is null                       as is_active
from {{ source('logistics', 'lanes') }}
