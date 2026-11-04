{{ config(materialized='incremental', incremental_strategy='append') }}
-- The catalogue attributes we track, stored only when they change: every object's first
-- appearance, then one row per change. This replaced a full copy of all ~71,000 objects for
-- every day, which made up most of the warehouse and grew by ~70,000 rows a day.
--
-- A day counts once it is complete (UTC), using its last download, so the several runs per
-- day don't record it several times. Comparing each day with the object's previous state
-- marks exactly where the old day-by-day history started a new version.
{% set today = "cast(now() at time zone 'UTC' as date)" %}

with days as (
    select *, cast(snapshot_at at time zone 'UTC' as date) as day
    from {{ source('bronze', 'satcat') }}
    where cast(snapshot_at at time zone 'UTC' as date) < {{ today }}
    {% if is_incremental() %}
      and cast(snapshot_at at time zone 'UTC' as date) > (
          select coalesce(max(snapshot_date), date '1900-01-01') from {{ this }}
      )
    {% endif %}
),

daily as (
    select *
    from days
    qualify row_number() over (partition by norad_cat_id, day order by snapshot_at desc) = 1
),

hashed as (
    select
        norad_cat_id,
        day as snapshot_date,
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
    from daily
),

{% if is_incremental() %}
stored as (
    select norad_cat_id, attr_hash
    from {{ this }}
    qualify row_number() over (partition by norad_cat_id order by snapshot_date desc) = 1
),
{% endif %}

compared as (
    select
        h.*,
        coalesce(
            lag(h.attr_hash) over (partition by h.norad_cat_id order by h.snapshot_date),
            {% if is_incremental() %} s.attr_hash {% else %} null {% endif %}
        ) as previous_hash
    from hashed as h
    {% if is_incremental() %}
    left join stored as s using (norad_cat_id)
    {% endif %}
)

select * exclude (previous_hash)
from compared
where previous_hash is distinct from attr_hash
