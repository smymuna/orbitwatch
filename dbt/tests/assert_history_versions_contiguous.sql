-- Each version must end exactly where the next one starts (no gaps, no overlaps).
select h.norad_cat_id, h.version, h.valid_to, n.valid_from
from {{ ref('dim_object_history') }} as h
join {{ ref('dim_object_history') }} as n
  on n.norad_cat_id = h.norad_cat_id and n.version = h.version + 1
where h.valid_to is distinct from n.valid_from
