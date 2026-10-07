-- Objects that have come back down, with when our catalogue snapshots first reported it.
with first_decay_version as (
    select norad_cat_id, min(valid_from) as reported_on
    from {{ ref('dim_object_history') }}
    where decay_date is not null
    group by norad_cat_id
),

tracking_start as (
    select min(snapshot_date) as first_snapshot from {{ ref('stg_satcat_daily') }}
)

select
    o.norad_cat_id,
    o.object_name,
    o.object_type,
    o.owner,
    o.launch_date,
    o.decay_date,
    o.decay_date - o.launch_date as days_in_orbit,
    f.reported_on,
    -- Re-entries reported in our very first snapshot happened before tracking started.
    f.reported_on = t.first_snapshot as known_before_tracking
from {{ ref('dim_object') }} as o
join first_decay_version as f using (norad_cat_id)
cross join tracking_start as t
