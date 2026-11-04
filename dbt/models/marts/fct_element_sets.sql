{{ config(
    materialized='incremental',
    unique_key=['norad_cat_id', 'epoch'],
    incremental_strategy='delete+insert',
) }}
-- One row per distinct element set (object + epoch), with derived orbit size and regime.
-- The same element set is usually downloaded in several snapshots until the object is
-- tracked again: keep it once, with when we first and last saw it.
--
-- Incremental: each run reads only snapshots newer than the last one processed. A set seen
-- again keeps its first_seen_at (and the attributes from that first download) and moves its
-- last_seen_at, so the result equals a full rebuild over every snapshot.
{% set cols %}
    norad_cat_id, object_name, object_id, epoch, mean_motion, eccentricity, inclination,
    ra_of_asc_node, arg_of_pericenter, mean_anomaly, bstar, mean_motion_dot, mean_motion_ddot,
    classification_type, ephemeris_type, element_set_no, rev_at_epoch, source_group
{% endset %}

with new_snapshots as (
    select *
    from {{ source('bronze', 'gp') }}
    {% if is_incremental() %}
    where snapshot_at > (
        select coalesce(max(last_seen_at), timestamptz '1970-01-01 00:00:00+00') from {{ this }}
    )
    {% endif %}
),

batch as (
    select
        {{ cols }},
        min(snapshot_at) over (partition by norad_cat_id, epoch) as first_seen_at,
        max(snapshot_at) over (partition by norad_cat_id, epoch) as last_seen_at
    from new_snapshots
    qualify row_number() over (partition by norad_cat_id, epoch order by snapshot_at) = 1
),

candidates as (
    select * from batch
    {% if is_incremental() %}
    union all
    select {{ cols }}, first_seen_at, last_seen_at
    from {{ this }} as t
    where exists (
        select 1 from batch as b where b.norad_cat_id = t.norad_cat_id and b.epoch = t.epoch
    )
    {% endif %}
),

merged as (
    select
        {{ cols }},
        first_seen_at,  -- the row kept below is the earliest, so this is the first download
        max(last_seen_at) over (partition by norad_cat_id, epoch) as last_seen_at,
        {{ semi_major_axis_km('mean_motion') }} as semi_major_axis_km,
        1440.0 / mean_motion as period_min
    from candidates
    qualify row_number() over (partition by norad_cat_id, epoch order by first_seen_at) = 1
),

sized as (
    select
        *,
        semi_major_axis_km * (1 - eccentricity) - 6378.137 as perigee_km,
        semi_major_axis_km * (1 + eccentricity) - 6378.137 as apogee_km
    from merged
)

select
    *,
    {{ orbit_regime('perigee_km', 'apogee_km', 'eccentricity', 'period_min') }} as orbit_regime
from sized
