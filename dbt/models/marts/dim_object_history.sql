-- Slowly changing dimension (type 2): one row per version of an object's attributes.
-- Built from every stored catalogue snapshot ("gaps and islands"), so it can be rebuilt
-- from scratch at any time instead of depending on when a dbt snapshot happened to run.
with daily as (
    select
        *,
        case
            when lag(attr_hash) over w is distinct from attr_hash then 1 else 0
        end as is_change
    from {{ ref('int_object_attributes_daily') }}
    window w as (partition by norad_cat_id order by snapshot_date)
),

versioned as (
    select
        *,
        sum(is_change) over (
            partition by norad_cat_id order by snapshot_date rows unbounded preceding
        ) as version
    from daily
),

versions as (
    select
        norad_cat_id,
        version,
        any_value(object_name) as object_name,
        any_value(object_type) as object_type,
        any_value(ops_status_code) as ops_status_code,
        any_value(owner) as owner,
        any_value(decay_date) as decay_date,
        any_value(orbit_type) as orbit_type,
        any_value(data_status_code) as data_status_code,
        min(snapshot_date) as valid_from,
        max(snapshot_date) as last_seen
    from versioned
    group by norad_cat_id, version
)

select
    norad_cat_id,
    version,
    object_name,
    object_type,
    ops_status_code,
    owner,
    decay_date,
    orbit_type,
    data_status_code,
    valid_from,
    lead(valid_from) over (partition by norad_cat_id order by version) as valid_to,
    lead(valid_from) over (partition by norad_cat_id order by version) is null as is_current
from versions
