"""End to end, offline: bronze snapshots for two days -> dbt models -> screening -> marts."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pyarrow as pa
import pytest

from orbitwatch.landing import parse_gp, parse_satcat, write_bronze
from tests.conftest import FIXTURES

DAY1 = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)
DAY2 = DAY1 + timedelta(days=1)
RAISED_KM = 2.0  # the ISS reboost we inject on day 2


def _gp(*names: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for n in names:
        out += json.loads((FIXTURES / f"gp_{n}.json").read_text())
    return out


def _write_history(data_dir: Path, days: tuple[int, ...] = (1, 2)) -> None:
    """Bronze snapshots for DAY1 and/or DAY2 (the same data whichever way it is split)."""
    bronze = data_dir / "bronze"
    day1 = _gp("stations", "cosmos-2251-debris")
    satcat = (FIXTURES / "satcat_sample.csv").read_bytes()
    t1 = parse_satcat(satcat, snapshot_at=DAY1)
    if 1 in days:
        write_bronze(
            bronze,
            "gp",
            "all",
            parse_gp(json.dumps(day1).encode(), group="all", snapshot_at=DAY1),
            DAY1,
        )
        write_bronze(bronze, "satcat", "satcat", t1, DAY1)
    if 2 not in days:
        return

    # Day 2: the ISS gets a new element set one day later with a 2 km higher orbit.
    # Mean motion n ~ a^-1.5, so a +2 km raise lowers n by 1.5 * 2 / a of itself.
    iss = next(r for r in day1 if r["NORAD_CAT_ID"] == 25544)
    epoch2 = datetime.fromisoformat(iss["EPOCH"]) + timedelta(days=1)
    a_km = 6378.137 + 420
    iss2 = dict(iss, EPOCH=epoch2.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                MEAN_MOTION=iss["MEAN_MOTION"] * (1 - 1.5 * RAISED_KM / a_km))  # fmt: skip
    write_bronze(
        bronze,
        "gp",
        "all",
        # The debris element sets are downloaded again unchanged, as happens until an object
        # is tracked again: they must keep their first_seen_at and move last_seen_at.
        parse_gp(
            json.dumps([iss2, *_gp("cosmos-2251-debris")]).encode(), group="all", snapshot_at=DAY2
        ),
        DAY2,
    )

    # Day 2: one Cosmos 2251 fragment re-enters (it gets a decay date).
    rows = t1.to_pylist()
    victim = next(r for r in rows if r["decay_date"] is None and r["object_type"] == "DEB")
    victim["decay_date"] = date(2026, 10, 7)
    for r in rows:
        r["snapshot_at"] = DAY2
    write_bronze(bronze, "satcat", "satcat", pa.Table.from_pylist(rows, schema=t1.schema), DAY2)
    (data_dir / "victim.txt").write_text(str(victim["norad_cat_id"]))


def _build(data_dir: Path) -> None:
    """Run the pipeline (minus downloads) over whatever bronze is in data_dir."""
    mp = pytest.MonkeyPatch()
    mp.setenv("ORBITWATCH_DATA_DIR", str(data_dir))  # read by the dbt profile
    try:
        from orbitwatch import definitions as d
        from orbitwatch.assets.compute import close_approaches
        from orbitwatch.assets.ingest import gp_snapshot, satcat_snapshot
        from orbitwatch.resources import OrbitwatchConfig

        ingest = dg.AssetSelection.assets(["bronze", "gp_snapshot"], ["bronze", "satcat_snapshot"])
        result = dg.materialize(
            [gp_snapshot, satcat_snapshot, d.orbitwatch_dbt, close_approaches],
            selection=dg.AssetSelection.all() - ingest,  # no network: bronze is pre-written
            resources={
                "dbt": d.dbt_resource,
                "orbitwatch": OrbitwatchConfig(
                    data_dir=str(data_dir), screening_start=DAY2.isoformat()
                ),
            },
            raise_on_error=True,
        )
        assert result.success
        checks = [e for e in result.get_asset_check_evaluations() if not e.passed]
        assert not checks, [str(c.asset_check_key) for c in checks]
    finally:
        mp.undo()


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, int]:
    data_dir = tmp_path_factory.mktemp("data")
    _write_history(data_dir)
    _build(data_dir)
    return data_dir / "warehouse.duckdb", int((data_dir / "victim.txt").read_text())


TABLES = [
    "fct_element_sets", "stg_satcat_latest", "stg_satcat_changes", "dim_object_history",
    "dim_object", "fct_orbit_changes", "fct_reentries", "mart_orbit_population",
    "mart_close_approaches", "mart_owners_in_orbit", "mart_launches_by_year",
]  # fmt: skip


def _contents(path: Path) -> dict[str, list[tuple[Any, ...]]]:
    with duckdb.connect(str(path), read_only=True) as con:
        return {t: sorted(con.sql(f"select * from {t}").fetchall(), key=repr) for t in TABLES}


def test_incremental_runs_equal_a_full_rebuild(warehouse: tuple[Path, int], tmp_path: Path) -> None:
    # Day 1, then day 2 added to the same warehouse, then a run with nothing new: the
    # result must match the warehouse built from both days at once.
    _write_history(tmp_path, days=(1,))
    _build(tmp_path)
    _write_history(tmp_path, days=(2,))
    _build(tmp_path)
    once = _contents(tmp_path / "warehouse.duckdb")
    _build(tmp_path)
    assert _contents(tmp_path / "warehouse.duckdb") == once  # nothing new, nothing changed
    assert once == _contents(warehouse[0])
    seen = _q(tmp_path / "warehouse.duckdb",
              "select min(first_seen_at), max(last_seen_at) from fct_element_sets "
              "where source_group = 'all' and norad_cat_id <> 25544")  # fmt: skip
    assert seen == [(DAY1, DAY2)]


def _q(path: Path, sql: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(path), read_only=True) as con:
        return con.sql(sql).fetchall()


def test_every_catalogued_object_has_one_current_row(warehouse: tuple[Path, int]) -> None:
    path, _ = warehouse
    (n_dim,) = _q(path, "select count(*) from dim_object")[0]
    (n_src,) = _q(path, "select count(distinct norad_cat_id) from stg_satcat_latest")[0]
    assert n_dim == n_src > 0


def test_reboost_is_detected_as_raise(warehouse: tuple[Path, int]) -> None:
    path, _ = warehouse
    sql = (
        "select change_type, delta_semi_major_axis_km from fct_orbit_changes where norad_cat_id = "
    )
    rows = _q(path, sql + "25544")
    assert len(rows) == 1
    change, delta = rows[0]
    assert change == "raise"
    assert delta == pytest.approx(RAISED_KM, rel=0.05)


def test_reentry_creates_a_new_history_version(warehouse: tuple[Path, int]) -> None:
    path, victim = warehouse
    versions = _q(
        path,
        "select decay_date, valid_from, valid_to, is_current from dim_object_history "
        f"where norad_cat_id = {victim} order by version",
    )
    assert len(versions) == 2
    (decay0, _, to0, current0), (decay1, from1, to1, current1) = versions
    assert decay0 is None
    assert decay1 == date(2026, 10, 7)
    assert to0 == from1 == DAY2.date()  # versions meet exactly, no gap or overlap
    assert not current0
    assert current1
    assert to1 is None

    sql = "select reported_on, known_before_tracking from fct_reentries where norad_cat_id = "
    assert _q(path, sql + str(victim)) == [(DAY2.date(), False)]
    # Sputnik's rocket re-entered in 1957, long before our tracking started.
    assert _q(path, sql + "1") == [(date(2026, 10, 6), True)]


def test_close_approaches_are_screened_and_named(warehouse: tuple[Path, int]) -> None:
    path, _ = warehouse
    rows = _q(
        path, "select object_a, object_b, miss_km from mart_close_approaches order by miss_km"
    )
    assert rows, "the debris fixture has close pairs"
    assert all(a is not None and b is not None for a, b, _ in rows)
    assert all(0 <= m < 5 for *_, m in rows)


def test_marts_answer_simple_questions(warehouse: tuple[Path, int]) -> None:
    path, _ = warehouse
    owners = dict(_q(path, "select owner, objects from mart_owners_in_orbit"))
    assert owners.get("CIS", 0) > 0  # Cosmos 2251 debris is Russian (CIS)
    years = dict(_q(path, "select launch_year, launches from mart_launches_by_year"))
    assert years[1957] >= 1
    regimes = {
        r for (r,) in _q(path, "select distinct orbit_regime from dim_object where is_in_orbit")
    }
    assert "LEO" in regimes


def test_warehouse_is_portable(warehouse: tuple[Path, int]) -> None:
    # The published warehouse must not depend on files on the machine that built it
    # (found when a released warehouse failed to query staging models).
    path, _ = warehouse
    leaky = _q(path, "select view_name from duckdb_views() where sql ilike '%read_parquet%'")
    assert leaky == []


def test_published_warehouse_opens_under_another_name(
    warehouse: tuple[Path, int], tmp_path: Path
) -> None:
    # Releases are named warehouse-YYYY-MM-DD.duckdb, so DuckDB gives the file another
    # catalog name; every table and view must still answer (a view naming the original
    # catalog broke this in the 2026-10-13 release).
    path, _ = warehouse
    copy = tmp_path / "warehouse-2026-10-13.duckdb"
    copy.write_bytes(path.read_bytes())
    with duckdb.connect(str(copy), read_only=True) as con:
        names = con.sql(
            "select schema_name, table_name from duckdb_tables() where not internal "
            "union all select schema_name, view_name from duckdb_views() where not internal"
        ).fetchall()
        assert names
        for schema, name in names:
            con.sql(f'select count(*) from "{schema}"."{name}"').fetchall()


def test_site_export(warehouse: tuple[Path, int], tmp_path: Path) -> None:
    from orbitwatch.export import ORBIT_FIELDS, export_site

    path, victim = warehouse
    counts = export_site(path, tmp_path)
    orbits = json.loads((tmp_path / "orbits.json").read_text())
    stats = json.loads((tmp_path / "stats.json").read_text())

    (n,) = _q(
        path, "select count(distinct norad_cat_id) from fct_element_sets where mean_motion > 0"
    )[0]
    assert counts["objects"] == len(orbits["objects"]) == n
    assert all(len(o) == len(ORBIT_FIELDS) for o in orbits["objects"])
    iss = next(o for o in orbits["objects"] if o[0] == 25544)
    assert iss[1].startswith("ISS")
    assert iss[ORBIT_FIELDS.index("type")] == 0  # payload
    # the ISS row is its latest element set: the raised orbit from day 2
    (mm,) = _q(path, "select mean_motion from fct_element_sets where norad_cat_id = 25544 "
                     "order by epoch desc limit 1")[0]  # fmt: skip
    assert iss[3] == round(mm, 8)
    misses = [c[2] for c in orbits["closest"]]
    assert misses == sorted(misses)
    assert orbits["pairs_5km"] == len(_q(path, "select 1 from mart_close_approaches"))
    assert orbits["snapshot_at"].startswith(DAY2.date().isoformat())

    assert victim in [r["norad"] for r in stats["reentries"]]
    assert stats["orbit_changes"]["by_type"].get("raise") == 1
    assert {"owner", "objects"} <= set(stats["owners"][0])


def test_compacted_copy_has_the_same_contents(
    warehouse: tuple[Path, int], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from orbitwatch.publish import main

    path, _ = warehouse
    copy = tmp_path / "warehouse-2026-10-07.duckdb"  # a release-style name
    main([str(path), str(copy)])
    assert capsys.readouterr().out.startswith("compacted ")
    assert copy.stat().st_size <= path.stat().st_size
    assert _contents(copy) == _contents(path)
