"""Orbit maths: derived orbital parameters, SGP4 propagation, close-approach screening.

Accuracy note: GP elements propagated with SGP4 are good to roughly 1 km at epoch and
degrade by a few km per day. Results here are *screening candidates* for exploring the
data, not collision probabilities; operators use covariance-based conjunction data.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
from scipy.spatial import cKDTree
from sgp4 import omm
from sgp4.api import Satrec, SatrecArray, jday

MU_KM3_S2 = 398_600.4418  # Earth's gravitational parameter
EARTH_RADIUS_KM = 6_378.137
MAX_RELATIVE_SPEED_KMS = 16.0  # head-on in low Earth orbit is about 15.5 km/s


def semi_major_axis_km(mean_motion_rev_per_day: float) -> float:
    n = mean_motion_rev_per_day * 2 * math.pi / 86_400  # rad/s
    return float((MU_KM3_S2 / n**2) ** (1 / 3))


def perigee_apogee_km(mean_motion_rev_per_day: float, eccentricity: float) -> tuple[float, float]:
    """Altitudes above the equatorial radius at the lowest and highest point of the orbit."""
    a = semi_major_axis_km(mean_motion_rev_per_day)
    return a * (1 - eccentricity) - EARTH_RADIUS_KM, a * (1 + eccentricity) - EARTH_RADIUS_KM


def satrec_from_record(rec: Mapping[str, Any]) -> Satrec:
    """Build an SGP4 satellite from a CelesTrak GP JSON record (OMM field names)."""
    fields = {k: str(v) for k, v in rec.items()}
    if "." not in fields["EPOCH"]:
        fields["EPOCH"] += ".000000"  # sgp4's OMM parser requires fractional seconds
    sat = Satrec()
    omm.initialize(sat, fields)
    return sat


@dataclass(frozen=True, slots=True)
class CloseApproach:
    norad_a: int
    norad_b: int
    tca: datetime  # time of closest approach
    miss_km: float
    relative_speed_kms: float


@dataclass(frozen=True, slots=True)
class ScreeningResult:
    approaches: list[CloseApproach]
    objects_screened: int
    objects_skipped: int  # stale elements or propagation errors
    co_moving_pairs: int  # docked or formation flying, excluded


def _julian(times: list[datetime]) -> tuple[np.ndarray, np.ndarray]:
    jd, fr = zip(
        *(
            jday(t.year, t.month, t.day, t.hour, t.minute, t.second + t.microsecond / 1e6)
            for t in times
        ),
        strict=True,
    )
    return np.array(jd), np.array(fr)


def screen_close_approaches(
    records: Iterable[Mapping[str, Any]],
    *,
    start: datetime,
    hours: float = 24.0,
    step_s: float = 20.0,
    threshold_km: float = 5.0,
    max_element_age_days: float = 14.0,
    co_moving_kms: float = 0.01,
    chunk_steps: int = 90,
) -> ScreeningResult:
    """Find pairs of objects passing within ``threshold_km`` over the next ``hours``.

    1. Propagate every object on a ``step_s`` time grid (vectorised SGP4, in chunks to
       bound memory).
    2. At each step, a KD-tree returns pairs closer than the threshold plus the distance
       two objects can close in half a step; nothing else needs checking.
    3. For each candidate pair, assume straight-line relative motion within the step and
       solve for the closest approach: t* = -(dr . dv) / |dv|^2, miss = |dr + dv t*|.
    4. Keep each pair's minimum. Pairs moving together (relative speed below
       ``co_moving_kms``, e.g. docked spacecraft) are counted, not reported.
    """
    start = start.astimezone(UTC)
    sats: list[Satrec] = []
    ids: list[int] = []
    skipped = 0
    seen: set[int] = set()
    for rec in records:
        norad = int(rec["NORAD_CAT_ID"])
        if norad in seen:
            continue  # the same object can appear in more than one group
        seen.add(norad)
        try:
            sat = satrec_from_record(rec)
        except (KeyError, ValueError):
            skipped += 1
            continue
        epoch = datetime(2000, 1, 1, tzinfo=UTC) + timedelta(
            days=sat.jdsatepoch + sat.jdsatepochF - 2451544.5
        )
        if abs((start - epoch).total_seconds()) > max_element_age_days * 86_400:
            skipped += 1  # propagating stale elements produces positions off by 100+ km
            continue
        sats.append(sat)
        ids.append(norad)

    if len(sats) < 2:
        return ScreeningResult([], len(sats), skipped, 0)

    array = SatrecArray(sats)
    id_arr = np.array(ids)
    n_steps = int(hours * 3600 / step_s) + 1
    radius = threshold_km + MAX_RELATIVE_SPEED_KMS * step_s / 2
    best: dict[tuple[int, int], CloseApproach] = {}
    co_moving: set[tuple[int, int]] = set()
    failed = np.zeros(len(sats), dtype=bool)

    for c0 in range(0, n_steps, chunk_steps):
        times = [
            start + timedelta(seconds=step_s * j) for j in range(c0, min(n_steps, c0 + chunk_steps))
        ]
        jd, fr = _julian(times)
        err, r, v = array.sgp4(jd, fr)  # shapes (n_sats, n_times), (n_sats, n_times, 3) x2
        failed |= (err != 0).any(axis=1)
        for j, t in enumerate(times):
            ok = ~failed & np.isfinite(r[:, j, 0])
            idx = np.flatnonzero(ok)
            if idx.size < 2:
                continue
            pos, vel = r[idx, j, :], v[idx, j, :]
            pairs = cKDTree(pos).query_pairs(radius, output_type="ndarray")
            if pairs.size == 0:
                continue
            a, b = pairs[:, 0], pairs[:, 1]
            dr = pos[b] - pos[a]
            dv = vel[b] - vel[a]
            dv2 = np.einsum("ij,ij->i", dv, dv)
            speed = np.sqrt(dv2)
            moving = speed >= co_moving_kms
            t_star = np.zeros_like(dv2)
            t_star[moving] = np.clip(
                -np.einsum("ij,ij->i", dr[moving], dv[moving]) / dv2[moving],
                -step_s / 2,
                step_s / 2,
            )
            miss = np.linalg.norm(dr + dv * t_star[:, None], axis=1)
            for k in np.flatnonzero(miss < threshold_km):
                pa_, pb_ = int(id_arr[idx[a[k]]]), int(id_arr[idx[b[k]]])
                key = (min(pa_, pb_), max(pa_, pb_))
                if not moving[k]:
                    co_moving.add(key)
                    continue
                found = CloseApproach(
                    key[0],
                    key[1],
                    t + timedelta(seconds=float(t_star[k])),
                    float(miss[k]),
                    float(speed[k]),
                )
                if key not in best or found.miss_km < best[key].miss_km:
                    best[key] = found

    approaches = sorted(
        (ca for key, ca in best.items() if key not in co_moving), key=lambda c: c.miss_km
    )
    return ScreeningResult(
        approaches, len(sats) - int(failed.sum()), skipped + int(failed.sum()), len(co_moving)
    )
