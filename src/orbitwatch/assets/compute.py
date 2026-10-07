"""Computations that are not SQL: close-approach screening with SGP4.

No `from __future__ import annotations` in asset modules: Dagster inspects the real
annotation types of asset parameters (context, resources) at definition time.
"""

from datetime import UTC, datetime

import dagster as dg
import duckdb
import pyarrow as pa

from orbitwatch.orbits import screen_close_approaches
from orbitwatch.resources import OrbitwatchConfig

# Latest element set per object, recent enough to propagate, renamed to OMM field names.
LATEST_ELEMENTS_SQL = """
select
    norad_cat_id      as "NORAD_CAT_ID",
    object_name       as "OBJECT_NAME",
    object_id         as "OBJECT_ID",
    strftime(epoch at time zone 'UTC', '%Y-%m-%dT%H:%M:%S.%f') as "EPOCH",
    mean_motion       as "MEAN_MOTION",
    eccentricity      as "ECCENTRICITY",
    inclination       as "INCLINATION",
    ra_of_asc_node    as "RA_OF_ASC_NODE",
    arg_of_pericenter as "ARG_OF_PERICENTER",
    mean_anomaly      as "MEAN_ANOMALY",
    ephemeris_type    as "EPHEMERIS_TYPE",
    classification_type as "CLASSIFICATION_TYPE",
    element_set_no    as "ELEMENT_SET_NO",
    rev_at_epoch      as "REV_AT_EPOCH",
    bstar             as "BSTAR",
    mean_motion_dot   as "MEAN_MOTION_DOT",
    mean_motion_ddot  as "MEAN_MOTION_DDOT"
from main.stg_gp_elements
qualify row_number() over (partition by norad_cat_id order by epoch desc) = 1
"""


@dg.asset(
    key=dg.AssetKey(["compute", "close_approaches"]),
    deps=[dg.AssetKey(["stg_gp_elements"])],
    group_name="compute",
    description="Pairs of objects predicted to pass within 5 km in the next 24 hours "
    "(SGP4 propagation + KD-tree screening). Screening candidates, not collision probabilities.",
    kinds={"python", "duckdb"},
)
def close_approaches(
    context: dg.AssetExecutionContext, orbitwatch: OrbitwatchConfig
) -> dg.MaterializeResult[None]:
    now = (
        datetime.fromisoformat(orbitwatch.screening_start)
        if orbitwatch.screening_start
        else datetime.now(UTC).replace(microsecond=0)
    )
    with duckdb.connect(str(orbitwatch.warehouse_path)) as con:
        records = con.sql(LATEST_ELEMENTS_SQL).to_arrow_table().to_pylist()
    context.log.info(f"screening {len(records)} objects for the next 24 h")

    result = screen_close_approaches(records, start=now)

    table = pa.table(
        {
            "screened_at": [now] * len(result.approaches),
            "norad_a": [c.norad_a for c in result.approaches],
            "norad_b": [c.norad_b for c in result.approaches],
            "tca": [c.tca for c in result.approaches],
            "miss_km": [round(c.miss_km, 3) for c in result.approaches],
            "relative_speed_kms": [round(c.relative_speed_kms, 3) for c in result.approaches],
        },
        schema=pa.schema(
            [
                ("screened_at", pa.timestamp("us", tz="UTC")),
                ("norad_a", pa.int64()),
                ("norad_b", pa.int64()),
                ("tca", pa.timestamp("us", tz="UTC")),
                ("miss_km", pa.float64()),
                ("relative_speed_kms", pa.float64()),
            ]
        ),
    )
    with duckdb.connect(str(orbitwatch.warehouse_path)) as con:
        con.execute("create schema if not exists compute")
        con.register("result", table)
        con.execute("create or replace table compute.close_approaches as select * from result")

    closest = result.approaches[0] if result.approaches else None
    return dg.MaterializeResult(
        metadata={
            "objects_screened": result.objects_screened,
            "objects_skipped": result.objects_skipped,
            "co_moving_pairs_excluded": result.co_moving_pairs,
            "close_approaches": len(result.approaches),
            "closest_km": round(closest.miss_km, 3) if closest else None,
        }
    )
