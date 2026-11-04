{{ config(materialized='incremental', incremental_strategy='append') }}
-- How many objects are in orbit on each snapshot day, by type and orbit regime.
-- Each completed UTC day is added once, from its last download; past days are kept, so
-- the series grows without re-reading old snapshots.
{% set today = "cast(now() at time zone 'UTC' as date)" %}

with daily as (
    select *, cast(snapshot_at at time zone 'UTC' as date) as day
    from {{ source('bronze', 'satcat') }}
    where cast(snapshot_at at time zone 'UTC' as date) < {{ today }}
    {% if is_incremental() %}
      and cast(snapshot_at at time zone 'UTC' as date) > (
          select coalesce(max(snapshot_date), date '1900-01-01') from {{ this }}
      )
    {% endif %}
    qualify row_number() over (
        partition by norad_cat_id, cast(snapshot_at at time zone 'UTC' as date)
        order by snapshot_at desc
    ) = 1
)

select
    day as snapshot_date,
    object_type,
    coalesce(
        {{ orbit_regime('perigee_km', 'apogee_km', '0', 'period_min') }}, 'UNKNOWN'
    ) as orbit_regime,
    count(*) as objects
from daily
where decay_date is null or decay_date > day
group by all
