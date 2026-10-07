-- Orbit changes between consecutive element sets of the same object.
--
-- Atmospheric drag only ever lowers an orbit, and slowly (well under 1 km a day above
-- ~350 km), so:
--   raise         semi-major axis up by more than raise_km      -> engine burn
--   lower         down by more than lower_km above 300 km        -> deliberate lowering
--   plane_change  inclination changed by more than plane_change_deg
-- Gaps longer than max_gap_days are ignored: the change could be spread over weeks.
-- These are heuristics; element-set noise is around 0.1 km in semi-major axis.
with pairs as (
    select
        norad_cat_id,
        object_name,
        epoch,
        semi_major_axis_km,
        inclination,
        perigee_km,
        lag(epoch) over w as prev_epoch,
        lag(semi_major_axis_km) over w as prev_semi_major_axis_km,
        lag(inclination) over w as prev_inclination
    from {{ ref('fct_element_sets') }}
    window w as (partition by norad_cat_id order by epoch)
),

deltas as (
    select
        *,
        semi_major_axis_km - prev_semi_major_axis_km as delta_semi_major_axis_km,
        inclination - prev_inclination as delta_inclination_deg,
        date_diff('minute', prev_epoch, epoch) / 1440.0 as gap_days
    from pairs
    where prev_epoch is not null
)

select
    norad_cat_id,
    object_name,
    prev_epoch,
    epoch,
    gap_days,
    delta_semi_major_axis_km,
    delta_inclination_deg,
    perigee_km,
    case
        when delta_semi_major_axis_km > {{ var('raise_km') }} then 'raise'
        when delta_semi_major_axis_km < -{{ var('lower_km') }} and perigee_km > 300 then 'lower'
        when abs(delta_inclination_deg) > {{ var('plane_change_deg') }} then 'plane_change'
    end as change_type
from deltas
where gap_days <= {{ var('max_gap_days') }}
  and (
      delta_semi_major_axis_km > {{ var('raise_km') }}
      or (delta_semi_major_axis_km < -{{ var('lower_km') }} and perigee_km > 300)
      or abs(delta_inclination_deg) > {{ var('plane_change_deg') }}
  )
