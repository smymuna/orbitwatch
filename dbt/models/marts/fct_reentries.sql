-- Objects that have come back down, with when our catalogue snapshots first reported it.
-- History only covers completed days, so a re-entry reported today falls back to the
-- current catalogue's date.
with first_decay_version as (
    select norad_cat_id, min(valid_from) as reported_on
    from {{ ref('dim_object_history') }}
    where decay_date is not null
    group by norad_cat_id
),

tracking_start as (
    select min(valid_from) as first_snapshot from {{ ref('dim_object_history') }}
)

select
    o.norad_cat_id,
    o.object_name,
    o.object_type,
    o.owner,
    o.launch_date,
    o.decay_date,
    o.decay_date - o.launch_date as days_in_orbit,
    coalesce(f.reported_on, o.catalogue_as_of) as reported_on,
    -- Re-entries reported in our very first snapshot happened before tracking started.
    coalesce(f.reported_on, o.catalogue_as_of) = t.first_snapshot as known_before_tracking
from {{ ref('dim_object') }} as o
left join first_decay_version as f using (norad_cat_id)
cross join tracking_start as t
where o.decay_date is not null
