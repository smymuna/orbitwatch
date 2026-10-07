"""Regression tests for two bugs that only showed outside a dev checkout (found in CI)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _load_definitions(env: dict[str, str], cwd: Path) -> str:
    code = (
        "import os, orbitwatch.definitions as d; "
        "print(d.DBT_DIR); print(os.environ['ORBITWATCH_DATA_DIR'])"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=cwd, env={**os.environ, **env}, check=True,
        capture_output=True, text=True,
    )  # fmt: skip
    return out.stdout


def test_relative_data_dir_is_made_absolute_for_dbt() -> None:
    # The daily job sets ORBITWATCH_DATA_DIR=data; dbt runs inside dbt/, where "data"
    # would mean dbt/data. The definitions pin it to an absolute path.
    dbt_dir, data_dir = _load_definitions({"ORBITWATCH_DATA_DIR": "data"}, REPO).splitlines()
    assert Path(data_dir).is_absolute()
    assert Path(data_dir) == REPO / "data"
    assert Path(dbt_dir) == REPO / "dbt"


def test_dbt_dir_can_be_set_explicitly(tmp_path: Path) -> None:
    # A non-editable install (the Docker image) can't find dbt/ relative to the package.
    dbt_dir, _ = _load_definitions({"ORBITWATCH_DBT_DIR": str(REPO / "dbt")}, tmp_path).splitlines()
    assert Path(dbt_dir) == REPO / "dbt"
