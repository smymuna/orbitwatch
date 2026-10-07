-- How many objects are in orbit on each snapshot day, by type and orbit regime.
select
    snapshot_date,
    object_type,
    coalesce(
        {{ orbit_regime('perigee_km', 'apogee_km', '0', 'period_min') }}, 'UNKNOWN'
    ) as orbit_regime,
    count(*) as objects
from {{ ref('stg_satcat_daily') }}
where decay_date is null or decay_date > snapshot_date
group by all
