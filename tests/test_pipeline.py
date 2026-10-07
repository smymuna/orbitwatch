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


def _write_history(data_dir: Path) -> None:
    bronze = data_dir / "bronze"
    day1 = _gp("stations", "cosmos-2251-debris")
    write_bronze(
        bronze,
        "gp",
        "all",
        parse_gp(json.dumps(day1).encode(), group="all", snapshot_at=DAY1),
        DAY1,
    )

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
        parse_gp(json.dumps([iss2]).encode(), group="all", snapshot_at=DAY2),
        DAY2,
    )

    satcat = (FIXTURES / "satcat_sample.csv").read_bytes()
    t1 = parse_satcat(satcat, snapshot_at=DAY1)
    write_bronze(bronze, "satcat", "satcat", t1, DAY1)
    # Day 2: one Cosmos 2251 fragment re-enters (it gets a decay date).
    rows = t1.to_pylist()
    victim = next(r for r in rows if r["decay_date"] is None and r["object_type"] == "DEB")
    victim["decay_date"] = date(2026, 10, 7)
    for r in rows:
        r["snapshot_at"] = DAY2
    write_bronze(bronze, "satcat", "satcat", pa.Table.from_pylist(rows, schema=t1.schema), DAY2)
    (data_dir / "victim.txt").write_text(str(victim["norad_cat_id"]))


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, int]:
    data_dir = tmp_path_factory.mktemp("data")
    _write_history(data_dir)
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
    return data_dir / "warehouse.duckdb", int((data_dir / "victim.txt").read_text())


def _q(path: Path, sql: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(path), read_only=True) as con:
        return con.sql(sql).fetchall()


def test_every_catalogued_object_has_one_current_row(warehouse: tuple[Path, int]) -> None:
    path, _ = warehouse
    (n_dim,) = _q(path, "select count(*) from dim_object")[0]
    (n_src,) = _q(path, "select count(distinct norad_cat_id) from stg_satcat_daily")[0]
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
