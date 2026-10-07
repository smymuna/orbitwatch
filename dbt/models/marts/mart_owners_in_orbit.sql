-- Who owns what is in orbit today.
select
    owner,
    count(*) as objects,
    count(*) filter (where object_type = 'PAY') as payloads,
    count(*) filter (where object_type = 'DEB') as debris,
    count(*) filter (where object_type = 'R/B') as rocket_bodies
from {{ ref('dim_object') }}
where is_in_orbit
group by owner
order by objects desc
