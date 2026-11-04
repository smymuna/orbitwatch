-- Slowly changing dimension (type 2): one row per version of an object's attributes.
-- stg_satcat_changes holds exactly the days a version starts, so each change row is a
-- version and it ends where the next one starts. Rebuilt in full on every run (one row
-- per version, ~72,000 rows), so it never depends on when a dbt snapshot happened to run.
select
    norad_cat_id,
    row_number() over w as version,
    object_name,
    object_type,
    ops_status_code,
    owner,
    decay_date,
    orbit_type,
    data_status_code,
    snapshot_date as valid_from,
    lead(snapshot_date) over w as valid_to,
    lead(snapshot_date) over w is null as is_current
from {{ ref('stg_satcat_changes') }}
window w as (partition by norad_cat_id order by snapshot_date)
