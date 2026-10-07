-- Launches and payloads per year, from the international designator (YYYY-NNN piece).
select
    year(launch_date) as launch_year,
    count(distinct left(object_id, 8)) as launches,
    count(*) filter (where object_type = 'PAY') as payloads,
    count(*) filter (where object_type = 'PAY' and object_name like 'STARLINK%') as starlink_payloads,
    count(*) filter (where object_type = 'DEB') as debris_catalogued
from {{ ref('dim_object') }}
where launch_date is not null and object_id is not null
group by all
order by launch_year
