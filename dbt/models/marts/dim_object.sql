-- Current view of every object ever catalogued, with its latest orbit when we have one.
with latest_catalogue as (
    select *
    from {{ ref('stg_satcat_daily') }}
    qualify row_number() over (partition by norad_cat_id order by snapshot_date desc) = 1
),

latest_elements as (
    select *
    from {{ ref('fct_element_sets') }}
    qualify row_number() over (partition by norad_cat_id order by epoch desc) = 1
)

select
    c.norad_cat_id,
    c.object_name,
    c.object_id,
    c.object_type,
    c.owner,
    c.launch_date,
    c.launch_site,
    c.decay_date,
    c.decay_date is null as is_in_orbit,
    c.ops_status_code,
    c.rcs_m2,
    case
        when c.rcs_m2 is null then 'unknown'
        when c.rcs_m2 < 0.1 then 'small'
        when c.rcs_m2 < 1.0 then 'medium'
        else 'large'
    end as size_class,
    coalesce(e.perigee_km, c.perigee_km) as perigee_km,
    coalesce(e.apogee_km, c.apogee_km) as apogee_km,
    coalesce(e.inclination, c.inclination) as inclination,
    coalesce(e.period_min, c.period_min) as period_min,
    coalesce(
        e.orbit_regime,
        case when c.decay_date is null then
            {{ orbit_regime('c.perigee_km', 'c.apogee_km', '0', 'c.period_min') }}
        end
    ) as orbit_regime,
    e.epoch as latest_epoch,
    e.mean_motion_dot as latest_mean_motion_dot,
    c.snapshot_date as catalogue_as_of
from latest_catalogue as c
left join latest_elements as e using (norad_cat_id)
