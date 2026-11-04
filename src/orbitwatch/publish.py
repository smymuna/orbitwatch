"""Write a compact copy of the warehouse for publishing.

Tables are rebuilt or rewritten on every run, and DuckDB keeps the freed space inside the
file rather than shrinking it (about 20 % of a published warehouse was free space).
Copying the database into a new file drops that space. The warehouse has no views (they
would name the original catalog), so a plain copy of every table is complete.

    orbitwatch-compact data/warehouse.duckdb warehouse-2026-11-04.duckdb
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def compact_warehouse(src: Path, dst: Path) -> tuple[int, int]:
    """Copy ``src`` into a new file ``dst``; return both sizes in bytes."""
    dst.unlink(missing_ok=True)
    with duckdb.connect() as con:
        con.execute(f"attach '{src}' as source_db (read_only)")
        con.execute(f"attach '{dst}' as compact_db")
        con.execute("copy from database source_db to compact_db")
    return src.stat().st_size, dst.stat().st_size


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("src", type=Path)
    parser.add_argument("dst", type=Path)
    args = parser.parse_args(argv)
    before, after = compact_warehouse(args.src, args.dst)
    print(f"compacted {before / 2**20:.1f} MB -> {after / 2**20:.1f} MB")


if __name__ == "__main__":
    main()
