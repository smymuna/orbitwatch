-- One row per distinct element set (object + epoch). The same element set is usually
-- downloaded in several snapshots until the object is tracked again; keep it once,
-- with when we first and last saw it.
with ranked as (
    select
        *,
        row_number() over (partition by norad_cat_id, epoch order by snapshot_at) as rn,
        min(snapshot_at) over (partition by norad_cat_id, epoch) as first_seen_at,
        max(snapshot_at) over (partition by norad_cat_id, epoch) as last_seen_at
    from {{ source('bronze', 'gp') }}
),

derived as (
    select
        norad_cat_id,
        object_name,
        object_id,
        epoch,
        mean_motion,
        eccentricity,
        inclination,
        ra_of_asc_node,
        arg_of_pericenter,
        mean_anomaly,
        bstar,
        mean_motion_dot,
        mean_motion_ddot,
        classification_type,
        ephemeris_type,
        element_set_no,
        rev_at_epoch,
        source_group,
        first_seen_at,
        last_seen_at,
        {{ semi_major_axis_km('mean_motion') }} as semi_major_axis_km,
        1440.0 / mean_motion as period_min
    from ranked
    where rn = 1
)

select
    *,
    semi_major_axis_km * (1 - eccentricity) - 6378.137 as perigee_km,
    semi_major_axis_km * (1 + eccentricity) - 6378.137 as apogee_km
from derived
