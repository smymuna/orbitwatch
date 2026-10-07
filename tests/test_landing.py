from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from orbitwatch.landing import (
    ParseError,
    land_raw,
    parse_epoch,
    parse_gp,
    parse_satcat,
    write_bronze,
)
from orbitwatch.sources.celestrak import Download
from tests.conftest import FIXTURES

T = datetime(2026, 10, 7, 1, 40, 2, tzinfo=UTC)


def test_land_raw_is_gzipped_and_partitioned(tmp_path: Path) -> None:
    content = (FIXTURES / "gp_stations.json").read_bytes()
    landed = land_raw(tmp_path, Download("gp", "stations", "u", T, content))
    assert landed.path == tmp_path / "gp" / "group=stations" / "date=2026-10-07" / "014002Z.json.gz"
    assert gzip.decompress(landed.path.read_bytes()) == content
    assert landed.bytes == len(content)


def test_land_raw_refuses_not_modified(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not modified"):
        land_raw(tmp_path, Download("gp", "active", "u", T, None))


def test_parse_gp_real_payload() -> None:
    table = parse_gp((FIXTURES / "gp_stations.json").read_bytes(), group="stations", snapshot_at=T)
    rows = table.to_pylist()
    iss = next(r for r in rows if r["norad_cat_id"] == 25544)
    assert iss["object_name"] == "ISS (ZARYA)"
    assert iss["epoch"].tzinfo is not None
    assert 15 < iss["mean_motion"] < 16  # about 15.5 orbits a day
    assert {r["source_group"] for r in rows} == {"stations"}


def test_parse_gp_rejects_bad_records() -> None:
    bad = json.dumps([{"NORAD_CAT_ID": "x"}]).encode()
    with pytest.raises(ParseError, match="record 0"):
        parse_gp(bad, group="g", snapshot_at=T)
    with pytest.raises(ParseError, match="invalid JSON"):
        parse_gp(b"No GP data found", group="g", snapshot_at=T)


def test_parse_epoch_with_and_without_fraction() -> None:
    assert parse_epoch("2026-10-06T12:44:07.877472").microsecond == 877472
    assert parse_epoch("2026-10-06T12:44:07").second == 7


def test_parse_satcat_types_and_empty_cells() -> None:
    table = parse_satcat((FIXTURES / "satcat_sample.csv").read_bytes(), snapshot_at=T)
    rows = {r["norad_cat_id"]: r for r in table.to_pylist()}
    sputnik_rocket = rows[1]
    assert sputnik_rocket["launch_date"] == date(1957, 10, 4)
    assert sputnik_rocket["decay_date"] == date(1957, 12, 1)
    iss = rows[25544]
    assert iss["decay_date"] is None  # empty cell -> null, still in orbit
    assert iss["object_type"] == "PAY"


def test_parse_satcat_missing_column() -> None:
    with pytest.raises(ParseError, match="missing columns"):
        parse_satcat(b"OBJECT_NAME,NORAD_CAT_ID\nX,1\n", snapshot_at=T)


def test_write_bronze_hive_partition(tmp_path: Path) -> None:
    table = parse_gp((FIXTURES / "gp_stations.json").read_bytes(), group="stations", snapshot_at=T)
    path = write_bronze(tmp_path, "gp", "stations", table, T)
    assert path == tmp_path / "gp" / "snapshot_date=2026-10-07" / "stations_014002Z.parquet"
    assert pq.read_table(path).num_rows == table.num_rows
