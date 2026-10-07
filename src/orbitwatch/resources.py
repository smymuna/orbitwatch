from pathlib import Path

import dagster as dg

from orbitwatch.config import Settings


class OrbitwatchConfig(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Where data lives and what to download. Defaults come from ORBITWATCH_* variables."""

    data_dir: str = str(Settings().data_dir)
    gp_groups: list[str] = Settings().gp_groups
    http_timeout_s: float = Settings().http_timeout_s
    contact: str = Settings().contact
    pause_between_downloads_s: float = 2.0
    screening_start: str | None = None
    """ISO time to screen from instead of now (reproducible tests, backtesting)."""

    @property
    def raw_dir(self) -> Path:
        return Path(self.data_dir) / "raw"

    @property
    def bronze_dir(self) -> Path:
        return Path(self.data_dir) / "bronze"

    @property
    def warehouse_path(self) -> Path:
        return Path(self.data_dir) / "warehouse.duckdb"
