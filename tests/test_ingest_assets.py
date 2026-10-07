from __future__ import annotations

from pathlib import Path

import dagster as dg
import pyarrow.parquet as pq
import pytest
import respx

from orbitwatch.assets.ingest import (
    active_count_stable,
    gp_snapshot,
    satcat_is_complete,
    satcat_snapshot,
)
from orbitwatch.resources import OrbitwatchConfig
from orbitwatch.sources.celestrak import GP_URL, SATCAT_URL
from tests.conftest import FIXTURES


def _config(tmp_path: Path, groups: list[str]) -> OrbitwatchConfig:
    return OrbitwatchConfig(data_dir=str(tmp_path), gp_groups=groups, pause_between_downloads_s=0)


@respx.mock
def test_gp_snapshot_writes_changed_groups_and_skips_unchanged(tmp_path: Path) -> None:
    def respond(request):  # type: ignore[no-untyped-def]
        group = request.url.params["GROUP"]
        if group == "stations":
            return respx.MockResponse(200, content=(FIXTURES / "gp_stations.json").read_bytes())
        return respx.MockResponse(403, text=(FIXTURES / "gp_not_updated.txt").read_text())

    respx.get(GP_URL).mock(side_effect=respond)
    result = dg.materialize(
        [gp_snapshot], resources={"orbitwatch": _config(tmp_path, ["stations", "active"])}
    )
    assert result.success
    meta = result.asset_materializations_for_node("bronze__gp_snapshot")[0].metadata
    assert meta["objects_written"].value == 23
    assert meta["groups_unchanged"].value == ["active"]
    bronze = list((tmp_path / "bronze" / "gp").glob("*/stations_*.parquet"))
    assert len(bronze) == 1
    assert pq.read_metadata(bronze[0]).num_rows == 23
    assert list((tmp_path / "raw" / "gp" / "group=stations").glob("*/*.json.gz"))
    assert not list((tmp_path / "bronze" / "gp").glob("*/active_*.parquet"))  # unchanged: no file


@respx.mock
def test_gp_snapshot_keeps_good_groups_then_fails_for_retry(tmp_path: Path) -> None:
    def respond(request):  # type: ignore[no-untyped-def]
        if request.url.params["GROUP"] == "stations":
            return respx.MockResponse(200, content=(FIXTURES / "gp_stations.json").read_bytes())
        return respx.MockResponse(200, text=(FIXTURES / "gp_invalid_group.txt").read_text())

    respx.get(GP_URL).mock(side_effect=respond)
    result = dg.materialize(
        [gp_snapshot],
        resources={"orbitwatch": _config(tmp_path, ["stations", "typo-group"])},
        raise_on_error=False,
    )
    assert not result.success
    # the good group was still saved before failing
    assert list((tmp_path / "bronze" / "gp").glob("*/stations_*.parquet"))


@respx.mock
def test_satcat_downloads_once_per_day(tmp_path: Path) -> None:
    route = respx.get(SATCAT_URL).respond(content=(FIXTURES / "satcat_sample.csv").read_bytes())
    cfg = {"orbitwatch": _config(tmp_path, [])}
    first = dg.materialize([satcat_snapshot], resources=cfg)
    second = dg.materialize([satcat_snapshot], resources=cfg)
    assert route.call_count == 1
    meta2 = second.asset_materializations_for_node("bronze__satcat_snapshot")[0].metadata
    assert meta2["skipped"].value is True
    meta1 = first.asset_materializations_for_node("bronze__satcat_snapshot")[0].metadata
    assert meta1["objects"].value == 93


@respx.mock
def test_satcat_check_flags_an_incomplete_catalogue(tmp_path: Path) -> None:
    respx.get(SATCAT_URL).respond(content=(FIXTURES / "satcat_sample.csv").read_bytes())
    result = dg.materialize(
        [satcat_snapshot, satcat_is_complete], resources={"orbitwatch": _config(tmp_path, [])}
    )
    (evaluation,) = result.get_asset_check_evaluations()
    assert not evaluation.passed  # 93 rows is far below the real ~70,000
    assert evaluation.metadata["rows"].value == 93


@respx.mock
def test_active_count_check_warns_on_a_sudden_drop(tmp_path: Path) -> None:
    from datetime import UTC, datetime, timedelta

    from orbitwatch.landing import parse_gp, write_bronze

    full = (FIXTURES / "gp_stations.json").read_bytes()
    t0 = datetime(2026, 10, 6, tzinfo=UTC)
    table = parse_gp(full, group="active", snapshot_at=t0)
    write_bronze(tmp_path / "bronze", "gp", "active", table, t0)
    write_bronze(tmp_path / "bronze", "gp", "active", table.slice(0, 5), t0 + timedelta(hours=6))
    result = dg.materialize(
        [gp_snapshot, active_count_stable],
        selection=dg.AssetSelection.checks(active_count_stable),
        resources={"orbitwatch": _config(tmp_path, [])},
    )
    (evaluation,) = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.severity == dg.AssetCheckSeverity.WARN
    assert evaluation.metadata["change_pct"].value == pytest.approx(-78.26, abs=0.01)
