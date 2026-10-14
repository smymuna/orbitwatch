"""Export the warehouse as two small JSON files for the website (and the portfolio globe).

    orbits.json  latest element set of every object, compact rows the browser propagates
                 with SGP4, plus the closest predicted approaches
    stats.json   what the marts say: population, close approaches, re-entries,
                 manoeuvres, launches and owners

Run after the pipeline: ``orbitwatch-export --out site/data``.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from orbitwatch.config import Settings

TYPES = ["PAY", "DEB", "R/B", "UNK"]
ORBIT_FIELDS = ["norad", "name", "epoch_ms", "mean_motion", "ecc", "incl", "raan", "argp",
                "mean_anomaly", "bstar", "type", "owner"]  # fmt: skip

_LATEST_ELEMENTS = """
with latest as (
    select g.*, row_number() over (partition by g.norad_cat_id order by g.epoch desc) as rn
    from stg_gp_elements g
)
select l.norad_cat_id, l.object_name, epoch_ms(l.epoch), l.mean_motion, l.eccentricity,
       l.inclination, l.ra_of_asc_node, l.arg_of_pericenter, l.mean_anomaly, l.bstar,
       coalesce(d.object_type, 'UNK'), d.owner
from latest l left join dim_object d using (norad_cat_id)
where l.rn = 1 and l.mean_motion > 0
order by l.norad_cat_id
"""


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    rel = con.sql(sql)
    return [dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()]


def _ms(ts: datetime | None) -> int | None:
    return None if ts is None else int(ts.timestamp() * 1000)


def build_orbits(con: duckdb.DuckDBPyConnection, snapshot_at: datetime) -> dict[str, Any]:
    types = {t: i for i, t in enumerate(TYPES)}
    objects = [
        [
            r[0],
            r[1],
            r[2],
            round(r[3], 8),
            round(r[4], 7),
            round(r[5], 4),
            round(r[6], 4),
            round(r[7], 4),
            round(r[8], 4),
            float(f"{r[9]:.5g}"),
            types.get(r[10], 3),
            r[11],
        ]
        for r in con.sql(_LATEST_ELEMENTS).fetchall()
    ]
    closest = [
        [_ms(r[0]), _ms(r[1]), r[2], r[3], r[4], r[5], r[6], r[7]]
        for r in con.sql(
            "select screened_at, tca, miss_km, relative_speed_kms, norad_a, object_a, norad_b, "
            "object_b from mart_close_approaches order by miss_km limit 12"
        ).fetchall()
    ]
    in_orbit = con.sql(
        "select count(*) filter (where is_in_orbit),"
        " count(*) filter (where is_in_orbit and object_type = 'PAY'),"
        " count(*) filter (where is_in_orbit and object_type = 'DEB'),"
        " count(*) filter (where is_in_orbit and object_type = 'R/B') from dim_object"
    ).fetchone()
    (pairs,) = con.sql("select count(*) from mart_close_approaches").fetchone() or (0,)
    return {
        "snapshot": snapshot_at.date().isoformat(),
        "snapshot_at": snapshot_at.isoformat(),
        "fields": ORBIT_FIELDS,
        "types": TYPES,
        "objects": objects,
        "closest": closest,
        "in_orbit": list(in_orbit or (0, 0, 0, 0)),
        "pairs_5km": pairs,
    }


def build_stats(con: duckdb.DuckDBPyConnection, snapshot_at: datetime) -> dict[str, Any]:
    week = "(select max(epoch) from fct_element_sets) - interval 7 day"
    return {
        "snapshot_at": snapshot_at.isoformat(),
        "close_approaches": _rows(
            con,
            """
            select epoch_ms(tca) as tca_ms, round(miss_km, 3) as miss_km,
                   round(relative_speed_kms, 2) as speed_kms,
                   norad_a, object_a, type_a, owner_a, norad_b, object_b, type_b, owner_b
            from mart_close_approaches order by miss_km limit 25""",
        ),
        "reentries": _rows(
            con,
            """
            select norad_cat_id as norad, object_name as name, object_type as type, owner,
                   cast(decay_date as varchar) as decay_date, days_in_orbit
            from fct_reentries
            where decay_date >= (select max(decay_date) from fct_reentries) - interval 7 day
            order by decay_date desc, norad_cat_id limit 40""",
        ),
        "reentry_watch": _rows(
            con,
            """
            select norad_cat_id as norad, object_name as name, object_type as type, owner,
                   round(perigee_km) as perigee_km, round(apogee_km) as apogee_km
            from mart_reentry_watch order by perigee_km limit 25""",
        ),
        "orbit_changes": {
            "by_type": dict(
                con.sql(f"""
                select change_type, count(*) from fct_orbit_changes
                where epoch >= {week} group by 1 order by 1""").fetchall()
            ),
            "largest": _rows(
                con,
                f"""
                select norad_cat_id as norad, object_name as name, change_type,
                       round(delta_semi_major_axis_km, 2) as delta_km,
                       round(perigee_km) as perigee_km, epoch_ms(epoch) as epoch_ms
                from fct_orbit_changes where epoch >= {week}
                order by abs(delta_semi_major_axis_km) desc limit 12""",
            ),
        },
        "launches_by_year": _rows(
            con,
            """
            select launch_year as year, launches, payloads, starlink_payloads
            from mart_launches_by_year where launch_year >= 1957 order by launch_year""",
        ),
        "owners": _rows(
            con,
            """
            select owner, objects, payloads, debris, rocket_bodies
            from mart_owners_in_orbit order by objects desc limit 10""",
        ),
    }


def export_site(warehouse: Path, out_dir: Path) -> dict[str, int]:
    """Write orbits.json and stats.json; return a few counts for the log."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(warehouse), read_only=True) as con:
        con.execute("set timezone = 'UTC'")
        (last_seen,) = con.sql("select max(last_seen_at) from stg_gp_elements").fetchone() or (
            None,
        )
        snapshot_at = (last_seen or datetime.now(UTC)).astimezone(UTC)
        orbits = build_orbits(con, snapshot_at)
        stats = build_stats(con, snapshot_at)
    for name, data in (("orbits.json", orbits), ("stats.json", stats)):
        (out_dir / name).write_text(json.dumps(data, separators=(",", ":"), default=str))
    return {"objects": len(orbits["objects"]), "pairs_5km": orbits["pairs_5km"]}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--warehouse", type=Path, default=Settings().warehouse_path)
    parser.add_argument("--out", type=Path, default=Path("site/data"))
    args = parser.parse_args(argv)
    counts = export_site(args.warehouse, args.out)
    print(f"exported {counts['objects']} objects, {counts['pairs_5km']} close approaches")


if __name__ == "__main__":
    main()
