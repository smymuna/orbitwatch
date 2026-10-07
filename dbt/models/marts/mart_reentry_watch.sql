-- Objects still in orbit whose lowest point is under 250 km: drag will bring them down
-- within days to weeks. mean_motion_dot > 0 means the orbit is shrinking.
select
    norad_cat_id,
    object_name,
    object_type,
    owner,
    perigee_km,
    apogee_km,
    latest_mean_motion_dot,
    latest_epoch
from {{ ref('dim_object') }}
where is_in_orbit
  and perigee_km < 250
  and latest_epoch is not null
order by perigee_km
