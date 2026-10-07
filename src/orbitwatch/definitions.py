"""Dagster definitions: assets, checks, the refresh job and its schedule."""

import os
import shutil
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from orbitwatch.assets.compute import close_approaches
from orbitwatch.assets.ingest import (
    active_count_stable,
    gp_snapshot,
    satcat_is_complete,
    satcat_snapshot,
)
from orbitwatch.config import Settings
from orbitwatch.resources import OrbitwatchConfig


def find_dbt_dir() -> Path:
    """The dbt project: $ORBITWATCH_DBT_DIR, else next to the source tree, else ./dbt.

    "Two levels up from this file" only holds for an editable install; a normal install
    (as in the Docker image) puts this file in site-packages.
    """
    candidates = [
        os.environ.get("ORBITWATCH_DBT_DIR"),
        Path(__file__).resolve().parents[2] / "dbt",
        Path.cwd() / "dbt",
    ]
    for c in candidates:
        if c and (Path(c) / "dbt_project.yml").exists():
            return Path(c).resolve()
    raise FileNotFoundError("dbt project not found; set ORBITWATCH_DBT_DIR")


DBT_DIR = find_dbt_dir()
# dbt runs with the dbt project as its working directory, so a relative data directory
# would resolve to dbt/data. Pin it to an absolute path for both Python and dbt.
os.environ["ORBITWATCH_DATA_DIR"] = str(Settings().data_dir.resolve())
# The dbt installed next to this Python, so an un-activated virtualenv works too.
_venv_dbt = Path(sys.executable).with_name("dbt")
DBT_EXECUTABLE = str(_venv_dbt) if _venv_dbt.exists() else (shutil.which("dbt") or "dbt")

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_resource = DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE)
if not dbt_project.manifest_path.exists():
    # `dagster dev` would parse the project itself; CLI runs, tests and CI need it too.
    dbt_resource.cli(["parse", "--quiet"], target_path=dbt_project.target_path).wait()


class LayerTranslator(DagsterDbtTranslator):
    """Group dbt models by warehouse layer (their folder) instead of one "default" group."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        fqn = dbt_resource_props.get("fqn", [])
        return f"warehouse_{fqn[1]}" if len(fqn) > 2 else "warehouse"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=LayerTranslator(),
)
def orbitwatch_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Any:
    # `build` runs each model followed by its tests, which surface as Dagster asset checks.
    yield from dbt.cli(["build"], context=context).stream()


refresh_job = dg.define_asset_job(
    "refresh",
    selection=dg.AssetSelection.all(),
    description="Download new snapshots, rebuild the warehouse, screen close approaches.",
)

defs = dg.Definitions(
    assets=[gp_snapshot, satcat_snapshot, orbitwatch_dbt, close_approaches],
    asset_checks=[satcat_is_complete, active_count_stable],
    jobs=[refresh_job],
    schedules=[
        # Every 6 hours, at :17 rather than on the hour when everyone else hits the API.
        dg.ScheduleDefinition(
            job=refresh_job, cron_schedule="17 */6 * * *", execution_timezone="UTC"
        ),
    ],
    resources={
        "orbitwatch": OrbitwatchConfig(),
        "dbt": dbt_resource,
    },
)
