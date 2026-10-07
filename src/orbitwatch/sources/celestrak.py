"""CelesTrak client: current orbital elements (GP data) and the satellite catalogue (SATCAT).

CelesTrak refreshes GP data every 2 hours and asks clients not to download the same data
more often. A repeated request is answered with HTTP 403 and a plain-text message
("GP data has not updated since your last successful download ..."). That is not an
error for us: it means "nothing new", and the pipeline keeps the previous snapshot.
Clients that keep hammering the API get their IP address blocked, so there are no
automatic retries on 403.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from orbitwatch import __version__

log = logging.getLogger(__name__)

GP_URL = "https://celestrak.org/NORAD/elements/gp.php"
SATCAT_URL = "https://celestrak.org/pub/satcat.csv"
NOT_UPDATED_MARKER = "has not updated since your last successful"


class CelestrakError(Exception):
    """Download failed or returned something unexpected."""


@dataclass(frozen=True, slots=True)
class Download:
    source: str  # "gp" or "satcat"
    name: str  # GP group name, or "satcat"
    url: str
    fetched_at: datetime
    content: bytes | None
    """None when CelesTrak says the data has not changed since our last download."""

    @property
    def not_modified(self) -> bool:
        return self.content is None


def make_client(timeout_s: float, contact: str) -> httpx.Client:
    return httpx.Client(
        timeout=timeout_s,
        follow_redirects=True,
        headers={"User-Agent": f"orbitwatch/{__version__} (+{contact})"},
    )


def fetch_gp(http: httpx.Client, group: str) -> Download:
    """Current general perturbations (GP) elements for one group, as JSON (OMM fields)."""
    params = {"GROUP": group, "FORMAT": "json"}
    try:
        r = http.get(GP_URL, params=params)
    except httpx.TransportError as exc:
        raise CelestrakError(f"CelesTrak unreachable for group {group!r}: {exc}") from exc
    now = datetime.now(UTC)

    if r.status_code == 403 and NOT_UPDATED_MARKER in r.text:
        log.info("GP group %s not updated since last download; keeping previous snapshot", group)
        return Download("gp", group, str(r.url), now, None)
    if r.status_code != 200:
        raise CelestrakError(f"GP group {group!r}: HTTP {r.status_code}: {r.text[:200]!r}")
    if not r.content.lstrip().startswith(b"["):
        # Unknown groups answer 200 with a text message such as "No GP data found".
        raise CelestrakError(f"GP group {group!r}: unexpected response {r.text[:200]!r}")
    return Download("gp", group, str(r.url), now, r.content)


def fetch_satcat(http: httpx.Client) -> Download:
    """The full satellite catalogue: every object ever tracked, with launch and decay dates."""
    try:
        r = http.get(SATCAT_URL)
    except httpx.TransportError as exc:
        raise CelestrakError(f"CelesTrak unreachable for SATCAT: {exc}") from exc
    if r.status_code != 200:
        raise CelestrakError(f"SATCAT: HTTP {r.status_code}: {r.text[:200]!r}")
    if not r.content.startswith(b"OBJECT_NAME,"):
        raise CelestrakError(f"SATCAT: unexpected header {r.content[:80]!r}")
    return Download("satcat", "satcat", str(r.url), datetime.now(UTC), r.content)
