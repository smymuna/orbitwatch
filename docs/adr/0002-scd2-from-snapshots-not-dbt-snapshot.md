# ADR 0002: Slowly changing dimension from snapshots, not `dbt snapshot`

- Status: accepted
- Date: 2026-10-07

## Context

We want a type 2 history of catalogue attributes (status, owner, decay date, orbit type):
one row per version with `valid_from` / `valid_to`. dbt's built-in `snapshot` does this,
but it records changes relative to *when `dbt snapshot` runs*: it can't replay past
snapshots, and rebuilding from scratch loses history.

## Decision

`dim_object_history` is an ordinary dbt model built from **all** stored catalogue
snapshots using window functions ("gaps and islands"):

1. one row per object per snapshot day (the day's last download),
2. hash the tracked attributes, flag rows where the hash differs from the previous day,
3. a running sum of flags numbers the versions,
4. each version runs from its first day to the next version's first day.

## Consequences

- Fully reproducible: dropping the warehouse and rebuilding from bronze gives the same
  history, including after restoring archived snapshots.
- A value that changes and changes back (A -> B -> A) correctly yields three versions.
- Cost grows with the number of snapshots (about 70,000 rows a day). DuckDB handles years
  of this; at much larger scale an incremental model would replace the full rebuild.
- Tests assert that versions are contiguous (no gaps or overlaps) and unique.
