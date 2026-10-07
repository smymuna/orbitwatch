{# Orbital parameters from mean motion (rev/day) and eccentricity. Mirrors orbitwatch/orbits.py. #}

{% macro semi_major_axis_km(mean_motion) -%}
    power(398600.4418 / power({{ mean_motion }} * 2 * pi() / 86400, 2), 1.0 / 3)
{%- endmacro %}

{% macro orbit_regime(perigee_km, apogee_km, eccentricity, period_min) -%}
    case
        when {{ eccentricity }} >= 0.25 then 'HEO'
        when {{ apogee_km }} < 2000 then 'LEO'
        when {{ period_min }} between 1400 and 1480 and {{ eccentricity }} < 0.01 then 'GEO'
        when {{ perigee_km }} >= 2000 and {{ apogee_km }} < 35586 then 'MEO'
        else 'OTHER'
    end
{%- endmacro %}
