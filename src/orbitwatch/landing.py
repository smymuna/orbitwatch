"""Raw landing zone and typed Parquet ("bronze") files.

CelesTrak only serves the current state, so every snapshot we keep *is* the history:
raw bytes are saved exactly as received (gzip), then parsed into typed Parquet files
partitioned by snapshot date, which dbt reads with DuckDB.

    data/raw/gp/group=active/date=2026-10-07/014002Z.json.gz
    data/bronze/gp/snapshot_date=2026-10-07/active_014002Z.parquet
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from orbitwatch.sources.celestrak import Download


class ParseError(Exception):
    """A payload does not have the shape we expect; nothing is written for it."""


@dataclass(frozen=True, slots=True)
class Landed:
    path: Path
    sha256: str
    bytes: int


def _stamp(ts: datetime) -> tuple[str, str]:
    ts = ts.astimezone(UTC)
    return ts.strftime("%Y-%m-%d"), ts.strftime("%H%M%SZ")


def land_raw(raw_dir: Path, d: Download) -> Landed:
    """Write the raw payload, gzipped, atomically. Returns its path and checksum."""
    if d.content is None:
        raise ValueError("nothing to land: the download was not modified")
    day, clock = _stamp(d.fetched_at)
    ext = "json" if d.source == "gp" else "csv"
    folder = raw_dir / d.source / (f"group={d.name}" if d.source == "gp" else "") / f"date={day}"
    path = folder / f"{clock}.{ext}.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    tmp.write_bytes(gzip.compress(d.content, mtime=0))
    tmp.replace(path)
    return Landed(path, hashlib.sha256(d.content).hexdigest(), len(d.content))


# --- GP (orbital elements) -------------------------------------------------------------------

GP_SCHEMA = pa.schema(
    [
        ("norad_cat_id", pa.int64()),
        ("object_name", pa.string()),
        ("object_id", pa.string()),
        ("epoch", pa.timestamp("us", tz="UTC")),
        ("mean_motion", pa.float64()),  # revolutions per day
        ("eccentricity", pa.float64()),
        ("inclination", pa.float64()),  # degrees
        ("ra_of_asc_node", pa.float64()),
        ("arg_of_pericenter", pa.float64()),
        ("mean_anomaly", pa.float64()),
        ("ephemeris_type", pa.int64()),
        ("classification_type", pa.string()),
        ("element_set_no", pa.int64()),
        ("rev_at_epoch", pa.int64()),
        ("bstar", pa.float64()),
        ("mean_motion_dot", pa.float64()),
        ("mean_motion_ddot", pa.float64()),
        ("source_group", pa.string()),
        ("snapshot_at", pa.timestamp("us", tz="UTC")),
    ]
)

_GP_FLOATS = [
    "MEAN_MOTION", "ECCENTRICITY", "INCLINATION", "RA_OF_ASC_NODE", "ARG_OF_PERICENTER",
    "MEAN_ANOMALY", "BSTAR", "MEAN_MOTION_DOT", "MEAN_MOTION_DDOT",
]  # fmt: skip
_GP_INTS = ["NORAD_CAT_ID", "EPHEMERIS_TYPE", "ELEMENT_SET_NO", "REV_AT_EPOCH"]


def parse_epoch(value: str) -> datetime:
    """CelesTrak epochs are UTC without an offset, usually with microseconds."""
    fmt = "%Y-%m-%dT%H:%M:%S.%f" if "." in value else "%Y-%m-%dT%H:%M:%S"
    return datetime.strptime(value, fmt).replace(tzinfo=UTC)


def parse_gp(content: bytes, *, group: str, snapshot_at: datetime) -> pa.Table:
    try:
        records: list[dict[str, Any]] = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ParseError(f"GP group {group!r}: invalid JSON") from exc
    if not isinstance(records, list):
        raise ParseError(f"GP group {group!r}: expected a list of objects")

    cols: dict[str, list[Any]] = {f.name: [] for f in GP_SCHEMA}
    for i, rec in enumerate(records):
        try:
            for k in _GP_INTS:
                cols[k.lower()].append(int(rec[k]))
            for k in _GP_FLOATS:
                cols[k.lower()].append(float(rec[k]))
            cols["epoch"].append(parse_epoch(rec["EPOCH"]))
            cols["object_name"].append(rec["OBJECT_NAME"])
            cols["object_id"].append(rec["OBJECT_ID"])
            cols["classification_type"].append(rec["CLASSIFICATION_TYPE"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ParseError(f"GP group {group!r}, record {i}: {exc!r}") from exc
    n = len(records)
    cols["source_group"] = [group] * n
    cols["snapshot_at"] = [snapshot_at] * n
    return pa.table(cols, schema=GP_SCHEMA)


# --- SATCAT (catalogue) ----------------------------------------------------------------------

SATCAT_SCHEMA = pa.schema(
    [
        ("norad_cat_id", pa.int64()),
        ("object_name", pa.string()),
        ("object_id", pa.string()),
        ("object_type", pa.string()),  # PAY, R/B, DEB, UNK
        ("ops_status_code", pa.string()),
        ("owner", pa.string()),
        ("launch_date", pa.date32()),
        ("launch_site", pa.string()),
        ("decay_date", pa.date32()),
        ("period_min", pa.float64()),
        ("inclination", pa.float64()),
        ("apogee_km", pa.float64()),
        ("perigee_km", pa.float64()),
        ("rcs_m2", pa.float64()),
        ("data_status_code", pa.string()),
        ("orbit_center", pa.string()),
        ("orbit_type", pa.string()),
        ("snapshot_at", pa.timestamp("us", tz="UTC")),
    ]
)
SATCAT_COLUMNS = {
    "OBJECT_NAME", "OBJECT_ID", "NORAD_CAT_ID", "OBJECT_TYPE", "OPS_STATUS_CODE", "OWNER",
    "LAUNCH_DATE", "LAUNCH_SITE", "DECAY_DATE", "PERIOD", "INCLINATION", "APOGEE", "PERIGEE",
    "RCS", "DATA_STATUS_CODE", "ORBIT_CENTER", "ORBIT_TYPE",
}  # fmt: skip


def _opt_float(v: str) -> float | None:
    return float(v) if v.strip() else None


def _opt_date(v: str) -> date | None:
    return date.fromisoformat(v) if v.strip() else None


def _opt_str(v: str) -> str | None:
    return v.strip() or None


def parse_satcat(content: bytes, *, snapshot_at: datetime) -> pa.Table:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
    missing = SATCAT_COLUMNS - set(reader.fieldnames or [])
    if missing:
        raise ParseError(f"SATCAT is missing columns: {sorted(missing)}")
    cols: dict[str, list[Any]] = {f.name: [] for f in SATCAT_SCHEMA}
    for i, row in enumerate(reader):
        try:
            cols["norad_cat_id"].append(int(row["NORAD_CAT_ID"]))
            cols["object_name"].append(row["OBJECT_NAME"].strip())
            cols["object_id"].append(_opt_str(row["OBJECT_ID"]))
            cols["object_type"].append(_opt_str(row["OBJECT_TYPE"]))
            cols["ops_status_code"].append(_opt_str(row["OPS_STATUS_CODE"]))
            cols["owner"].append(_opt_str(row["OWNER"]))
            cols["launch_date"].append(_opt_date(row["LAUNCH_DATE"]))
            cols["launch_site"].append(_opt_str(row["LAUNCH_SITE"]))
            cols["decay_date"].append(_opt_date(row["DECAY_DATE"]))
            cols["period_min"].append(_opt_float(row["PERIOD"]))
            cols["inclination"].append(_opt_float(row["INCLINATION"]))
            cols["apogee_km"].append(_opt_float(row["APOGEE"]))
            cols["perigee_km"].append(_opt_float(row["PERIGEE"]))
            cols["rcs_m2"].append(_opt_float(row["RCS"]))
            cols["data_status_code"].append(_opt_str(row["DATA_STATUS_CODE"]))
            cols["orbit_center"].append(_opt_str(row["ORBIT_CENTER"]))
            cols["orbit_type"].append(_opt_str(row["ORBIT_TYPE"]))
        except (KeyError, ValueError) as exc:
            raise ParseError(f"SATCAT row {i + 2}: {exc!r}") from exc
    cols["snapshot_at"] = [snapshot_at] * len(cols["norad_cat_id"])
    return pa.table(cols, schema=SATCAT_SCHEMA)


def write_bronze(
    bronze_dir: Path, source: str, name: str, table: pa.Table, snapshot_at: datetime
) -> Path:
    """Write one snapshot as Parquet, partitioned by snapshot date (Hive style)."""
    day, clock = _stamp(snapshot_at)
    path = bronze_dir / source / f"snapshot_date={day}" / f"{name}_{clock}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)
    return path
