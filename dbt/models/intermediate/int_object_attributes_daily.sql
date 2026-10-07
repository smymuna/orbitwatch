-- The attributes whose changes we track over time, with a hash to detect changes cheaply.
select
    norad_cat_id,
    snapshot_date,
    object_name,
    object_type,
    ops_status_code,
    owner,
    decay_date,
    orbit_type,
    data_status_code,
    md5(concat_ws('|',
        object_name, object_type, ops_status_code, owner,
        cast(decay_date as varchar), orbit_type, data_status_code
    )) as attr_hash
from {{ ref('stg_satcat_daily') }}
