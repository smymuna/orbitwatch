{{ config(
    materialized='incremental',
    unique_key='norad_cat_id',
    incremental_strategy='delete+insert',
) }}
-- The newest catalogue row for every object ever catalogued (one row per object).
-- Incremental: each run reads only snapshots newer than the last one processed and
-- replaces the rows of the objects in them. The size stays at about one catalogue.
select
    * exclude (snapshot_date),
    cast(snapshot_at at time zone 'UTC' as date) as snapshot_date
from {{ source('bronze', 'satcat') }}
{% if is_incremental() %}
where snapshot_at > (
    select coalesce(max(snapshot_at), timestamptz '1970-01-01 00:00:00+00') from {{ this }}
)
{% endif %}
qualify row_number() over (partition by norad_cat_id order by snapshot_at desc) = 1
