from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def stations() -> list[dict[str, Any]]:
    """Real GP records for the space stations group (ISS, Tiangong and docked vehicles)."""
    data: list[dict[str, Any]] = json.loads((FIXTURES / "gp_stations.json").read_text())
    return data


@pytest.fixture
def debris() -> list[dict[str, Any]]:
    """Real GP records: 60 pieces of the Cosmos 2251 debris cloud."""
    data: list[dict[str, Any]] = json.loads((FIXTURES / "gp_cosmos-2251-debris.json").read_text())
    return data
