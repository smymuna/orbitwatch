from __future__ import annotations

import httpx
import pytest
import respx

from orbitwatch.sources.celestrak import GP_URL, SATCAT_URL, CelestrakError, fetch_gp, fetch_satcat
from tests.conftest import FIXTURES


@respx.mock
def test_gp_download() -> None:
    body = (FIXTURES / "gp_stations.json").read_bytes()
    route = respx.get(GP_URL).respond(content=body, headers={"content-type": "application/json"})
    with httpx.Client() as http:
        d = fetch_gp(http, "stations")
    assert d.content == body
    assert not d.not_modified
    assert route.calls[0].request.url.params["GROUP"] == "stations"
    assert route.calls[0].request.url.params["FORMAT"] == "json"


@respx.mock
def test_gp_not_updated_403_means_nothing_new_not_an_error() -> None:
    # Real reply when the same group is downloaded again within 2 hours.
    respx.get(GP_URL).respond(403, text=(FIXTURES / "gp_not_updated.txt").read_text())
    with httpx.Client() as http:
        d = fetch_gp(http, "active")
    assert d.not_modified
    assert d.content is None


@respx.mock
def test_gp_unknown_group_is_an_error_even_with_http_200() -> None:
    # Real reply for a group name that doesn't exist: HTTP 200 with a text message.
    respx.get(GP_URL).respond(200, text=(FIXTURES / "gp_invalid_group.txt").read_text())
    with httpx.Client() as http, pytest.raises(CelestrakError, match="unexpected response"):
        fetch_gp(http, "1982-092")


@respx.mock
def test_gp_other_403_is_an_error() -> None:
    respx.get(GP_URL).respond(403, text="Forbidden: your IP has been blocked")
    with httpx.Client() as http, pytest.raises(CelestrakError, match="HTTP 403"):
        fetch_gp(http, "active")


@respx.mock
def test_satcat_checks_the_header() -> None:
    respx.get(SATCAT_URL).respond(200, content=b"<html>maintenance</html>")
    with httpx.Client() as http, pytest.raises(CelestrakError, match="unexpected header"):
        fetch_satcat(http)


@respx.mock
def test_network_errors_are_wrapped() -> None:
    respx.get(GP_URL).mock(side_effect=httpx.ConnectTimeout("timeout"))
    with httpx.Client() as http, pytest.raises(CelestrakError, match="unreachable"):
        fetch_gp(http, "active")
