-- The catalogue as it was on each day we have a snapshot (the last download of the day).
with ranked as (
    select
        *,
        cast(snapshot_at as date) as snapshot_date,
        row_number() over (
            partition by norad_cat_id, cast(snapshot_at as date) order by snapshot_at desc
        ) as rn
    from {{ source('bronze', 'satcat') }}
)

select * exclude (rn)
from ranked
where rn = 1
