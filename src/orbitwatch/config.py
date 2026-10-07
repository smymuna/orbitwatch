from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration from ``ORBITWATCH_*`` environment variables (or ``.env``)."""

    model_config = SettingsConfigDict(env_prefix="ORBITWATCH_", env_file=".env", extra="ignore")

    data_dir: Path = Path("data")
    """Root for raw downloads (data/raw), parsed Parquet (data/bronze) and the warehouse."""

    gp_groups: list[str] = [
        "active",  # every operational satellite
        "cosmos-2251-debris",  # 2009 Iridium 33 / Cosmos 2251 collision
        "iridium-33-debris",
        "fengyun-1c-debris",  # 2007 Chinese anti-satellite test
        "cosmos-1408-debris",  # 2021 Russian anti-satellite test
    ]
    """CelesTrak GP groups to snapshot. Debris clouds matter for close-approach screening."""

    http_timeout_s: float = Field(default=120.0, gt=0)
    contact: str = "https://github.com/smymuna/orbitwatch"
    """Sent in the User-Agent so CelesTrak can see who is calling and how to reach us."""

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"
