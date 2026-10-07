-- An orbit's lowest point can never be above its highest point.
select norad_cat_id, epoch, perigee_km, apogee_km
from {{ ref('fct_element_sets') }}
where perigee_km > apogee_km + 0.001
