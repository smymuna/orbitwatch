"""Ingestion assets: download from CelesTrak, keep the raw bytes, write typed Parquet."""

import time
from datetime import UTC, datetime

import dagster as dg
import pyarrow.parquet as pq

from orbitwatch.landing import land_raw, parse_gp, parse_satcat, write_bronze
from orbitwatch.resources import OrbitwatchConfig
from orbitwatch.sources.celestrak import CelestrakError, fetch_gp, fetch_satcat, make_client

GP_KEY = dg.AssetKey(["bronze", "gp_snapshot"])
SATCAT_KEY = dg.AssetKey(["bronze", "satcat_snapshot"])


@dg.asset(
    key=GP_KEY,
    group_name="ingest",
    description="Current orbital elements for every configured CelesTrak group, appended as a "
    "new snapshot. Groups CelesTrak reports as unchanged are skipped, not re-downloaded.",
    kinds={"python", "parquet"},
)
def gp_snapshot(
    context: dg.AssetExecutionContext, orbitwatch: OrbitwatchConfig
) -> dg.MaterializeResult[None]:
    written: dict[str, int] = {}
    unchanged: list[str] = []
    errors: dict[str, str] = {}
    with make_client(orbitwatch.http_timeout_s, orbitwatch.contact) as http:
        for i, group in enumerate(orbitwatch.gp_groups):
            if i:
                time.sleep(orbitwatch.pause_between_downloads_s)  # be gentle with a free service
            try:
                d = fetch_gp(http, group)
            except CelestrakError as exc:
                errors[group] = str(exc)
                context.log.error(str(exc))
                continue
            if d.not_modified:
                unchanged.append(group)
                continue
            assert d.content is not None
            landed = land_raw(orbitwatch.raw_dir, d)
            table = parse_gp(d.content, group=group, snapshot_at=d.fetched_at)
            write_bronze(orbitwatch.bronze_dir, "gp", group, table, d.fetched_at)
            written[group] = table.num_rows
            context.log.info(f"{group}: {table.num_rows} objects -> {landed.path}")
    if errors:
        # Fail so the run is retried, but only after keeping what did download.
        raise dg.Failure(
            description=f"{len(errors)} group(s) failed: {', '.join(errors)}",
            metadata={
                "errors": dg.MetadataValue.json(errors),
                "written": dg.MetadataValue.json(written),
            },
        )
    return dg.MaterializeResult(
        metadata={
            "objects_written": sum(written.values()),
            "groups_written": dg.MetadataValue.json(written),
            "groups_unchanged": dg.MetadataValue.json(unchanged),
        }
    )


@dg.asset(
    key=SATCAT_KEY,
    group_name="ingest",
    description="The full satellite catalogue (every object ever tracked), one snapshot per day.",
    kinds={"python", "parquet"},
)
def satcat_snapshot(
    context: dg.AssetExecutionContext, orbitwatch: OrbitwatchConfig
) -> dg.MaterializeResult[None]:
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    existing = sorted(
        (orbitwatch.bronze_dir / "satcat" / f"snapshot_date={today}").glob("*.parquet")
    )
    if existing:
        context.log.info(f"catalogue already captured today ({existing[-1].name}); skipping")
        return dg.MaterializeResult(metadata={"skipped": True, "file": str(existing[-1])})
    with make_client(orbitwatch.http_timeout_s, orbitwatch.contact) as http:
        d = fetch_satcat(http)
    assert d.content is not None
    land_raw(orbitwatch.raw_dir, d)
    table = parse_satcat(d.content, snapshot_at=d.fetched_at)
    path = write_bronze(orbitwatch.bronze_dir, "satcat", "satcat", table, d.fetched_at)
    in_orbit = sum(1 for v in table.column("decay_date").to_pylist() if v is None)
    return dg.MaterializeResult(
        metadata={
            "objects": table.num_rows,
            "in_orbit": in_orbit,
            "file": str(path),
            "skipped": False,
        }
    )


@dg.asset_check(
    asset=SATCAT_KEY, description="The latest catalogue looks complete (>= 60,000 objects)."
)
def satcat_is_complete(orbitwatch: OrbitwatchConfig) -> dg.AssetCheckResult:
    files = sorted((orbitwatch.bronze_dir / "satcat").glob("*/*.parquet"))
    if not files:
        return dg.AssetCheckResult(passed=False, description="no catalogue snapshot yet")
    rows = pq.read_metadata(files[-1]).num_rows
    return dg.AssetCheckResult(
        passed=rows >= 60_000, metadata={"rows": rows, "file": files[-1].name}
    )


@dg.asset_check(
    asset=GP_KEY,
    description="The latest 'active' snapshot did not shrink by more than 10% "
    "(a sudden drop usually means a truncated download, not satellites vanishing).",
)
def active_count_stable(orbitwatch: OrbitwatchConfig) -> dg.AssetCheckResult:
    files = sorted((orbitwatch.bronze_dir / "gp").glob("*/active_*.parquet"))
    if len(files) < 2:
        return dg.AssetCheckResult(passed=True, description="not enough snapshots to compare yet")
    prev, last = (pq.read_metadata(f).num_rows for f in files[-2:])
    change = (last - prev) / prev
    return dg.AssetCheckResult(
        passed=change > -0.10,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"previous": prev, "latest": last, "change_pct": round(100 * change, 2)},
    )
