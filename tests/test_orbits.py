from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sgp4.api import jday

from orbitwatch.orbits import (
    perigee_apogee_km,
    satrec_from_record,
    screen_close_approaches,
    semi_major_axis_km,
)

T0 = datetime(2026, 10, 7, 2, 0, tzinfo=UTC)


def _circular(
    norad: int, *, raan: float, mean_anomaly: float, inclination: float = 53.0
) -> dict[str, Any]:
    """A synthetic circular orbit at ~550 km (Starlink-like), epoch at T0."""
    return {
        "OBJECT_NAME": f"TEST-{norad}", "OBJECT_ID": "2026-001A", "NORAD_CAT_ID": norad,
        "EPOCH": "2026-10-07T02:00:00.000000", "MEAN_MOTION": 15.06, "ECCENTRICITY": 0.0001,
        "INCLINATION": inclination, "RA_OF_ASC_NODE": raan, "ARG_OF_PERICENTER": 0.0,
        "MEAN_ANOMALY": mean_anomaly, "EPHEMERIS_TYPE": 0, "CLASSIFICATION_TYPE": "U",
        "ELEMENT_SET_NO": 999, "REV_AT_EPOCH": 1, "BSTAR": 0.0, "MEAN_MOTION_DOT": 0.0,
        "MEAN_MOTION_DDOT": 0.0,
    }  # fmt: skip


def test_semi_major_axis_geostationary() -> None:
    # One revolution per sidereal day is the geostationary radius, ~42,164 km.
    assert semi_major_axis_km(1.0027379) == pytest.approx(42_164, abs=2)


def test_iss_perigee_apogee(stations: list[dict[str, Any]]) -> None:
    iss = next(r for r in stations if r["NORAD_CAT_ID"] == 25544)
    perigee, apogee = perigee_apogee_km(iss["MEAN_MOTION"], iss["ECCENTRICITY"])
    assert 380 < perigee < apogee < 450


def _brute_force_min_distance(recs: list[dict[str, Any]], around: datetime, window_s: int) -> float:
    """Independent check: propagate both objects every second and take the minimum."""
    sa, sb = (satrec_from_record(r) for r in recs)
    best = math.inf
    for s in range(-window_s, window_s + 1):
        t = around + timedelta(seconds=s)
        jd, fr = jday(t.year, t.month, t.day, t.hour, t.minute, t.second)
        _, ra, _ = sa.sgp4(jd, fr)
        _, rb, _ = sb.sgp4(jd, fr)
        best = min(best, math.dist(ra, rb))
    return best


def test_crossing_orbits_match_brute_force() -> None:
    # Same altitude, perpendicular planes, both at the shared node at T0. SGP4's Earth-
    # oblateness (J2) terms shift the two differently, so they actually pass ~16 km apart,
    # not 0. The screening's linear refinement must agree with a 1-second brute force.
    a = _circular(90001, raan=0.0, mean_anomaly=0.0, inclination=0.0001)
    b = _circular(90002, raan=0.0, mean_anomaly=0.0, inclination=90.0)
    res = screen_close_approaches([a, b], start=T0, hours=0.5, threshold_km=25)
    assert len(res.approaches) == 1
    ca = res.approaches[0]
    expected = _brute_force_min_distance([a, b], ca.tca, window_s=30)
    assert ca.miss_km == pytest.approx(expected, abs=0.05)
    assert abs((ca.tca - T0).total_seconds()) < 60
    # perpendicular circular orbits at ~7.6 km/s each: relative speed ~ 7.6 * sqrt(2)
    assert ca.relative_speed_kms == pytest.approx(7.6 * math.sqrt(2), rel=0.05)
    # and with a 5 km threshold it is (correctly) not reported
    assert screen_close_approaches([a, b], start=T0, hours=0.5, threshold_km=5).approaches == []


def test_far_apart_orbits_are_not_reported() -> None:
    a = _circular(90001, raan=0.0, mean_anomaly=0.0)
    b = _circular(90002, raan=180.0, mean_anomaly=180.0)
    assert screen_close_approaches([a, b], start=T0, hours=1).approaches == []


def test_docked_vehicles_are_co_moving_not_conjunctions(stations: list[dict[str, Any]]) -> None:
    res = screen_close_approaches(stations, start=T0, hours=2)
    assert res.co_moving_pairs > 0  # ISS and Tiangong with their docked vehicles
    for ca in res.approaches:
        assert ca.relative_speed_kms >= 0.01


def test_stale_and_duplicate_records_are_skipped(debris: list[dict[str, Any]]) -> None:
    old = dict(debris[0], NORAD_CAT_ID=99999, EPOCH="2025-01-01T00:00:00.000000")
    res = screen_close_approaches([*debris, old, debris[1]], start=T0, hours=0.1)
    assert res.objects_skipped == 1  # the stale one; the duplicate is silently merged
    assert res.objects_screened == len(debris)


def test_real_debris_cloud_finds_known_close_pairs(debris: list[dict[str, Any]]) -> None:
    res = screen_close_approaches(debris, start=T0, threshold_km=5)
    assert res.approaches, "the Cosmos 2251 cloud has close pairs within a day"
    misses = [c.miss_km for c in res.approaches]
    assert misses == sorted(misses)
    assert all(0 <= m < 5 for m in misses)
