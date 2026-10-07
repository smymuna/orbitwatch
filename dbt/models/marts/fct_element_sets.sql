select
    *,
    {{ orbit_regime('perigee_km', 'apogee_km', 'eccentricity', 'period_min') }} as orbit_regime
from {{ ref('stg_gp_elements') }}
