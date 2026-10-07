-- Close-approach screening results (computed in Python) with object details.
select
    ca.screened_at,
    ca.tca,
    ca.miss_km,
    ca.relative_speed_kms,
    ca.norad_a,
    a.object_name as object_a,
    a.object_type as type_a,
    a.owner as owner_a,
    ca.norad_b,
    b.object_name as object_b,
    b.object_type as type_b,
    b.owner as owner_b
from {{ source('compute', 'close_approaches') }} as ca
left join {{ ref('dim_object') }} as a on a.norad_cat_id = ca.norad_a
left join {{ ref('dim_object') }} as b on b.norad_cat_id = ca.norad_b
