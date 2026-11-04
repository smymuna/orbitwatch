# ADR 0004: An incremental warehouse that keeps all history

- Status: accepted
- Date: 2026-11-04

## Context

Every run rebuilt the warehouse from scratch out of the raw snapshots of the last 90 data
releases. By 4 November 2026 the published warehouse was 274 MB and growing by 7–8 MB a
day, for three reasons:

- `stg_satcat_daily` kept a full copy of the catalogue (~71,000 objects) for every day:
  2.06 million rows after 29 days, although only a few hundred attributes change per day.
- `fct_element_sets` was `stg_gp_elements` plus one column: the same 810,000 rows twice.
- About 20 % of the file was free space left by rebuilding tables.

Worse, the 90-release restore meant that from early January 2027 history older than
90 days would have silently dropped out of the warehouse, although the README promises the
full history.

## Decision

- **Continue from the last warehouse.** The Collect workflow downloads the newest published
  warehouse and only the snapshots from that day on. A full rebuild from every snapshot
  happens when there is no usable warehouse (first run, a warehouse from before this
  change), or on request (`full_refresh` input, needed after changing an incremental model).
- **Incremental models** read only snapshots newer than what they hold:
  - `fct_element_sets` (merge on object + epoch): a set downloaded again keeps its
    `first_seen_at` and attributes and moves its `last_seen_at`. `stg_gp_elements` is gone;
    its logic lives in this model.
  - `stg_satcat_latest` (merge on object): the newest catalogue row per object.
  - `stg_satcat_changes` (append): tracked attributes only when they change. Each completed
    UTC day is compared with the object's previous state, so the change rows are exactly
    the days the old day-by-day history started a new version (ADR 0002 still describes the
    meaning; the change detection now happens once per day instead of on every rebuild).
  - `mart_orbit_population` (append): one set of rows per completed day.
- **Completed days only** for the change log and the population series: four runs a day
  must not record a day four times. `dim_object` and re-entries stay current within the
  day through `stg_satcat_latest`.
- **Compact before publishing** (`orbitwatch-compact`, a `COPY FROM DATABASE` into a new
  file).

## Verification

On the real snapshots (7 Oct – 4 Nov 2026): a full build with the new models matches the
warehouse built by the old code in every table, apart from floating-point rounding below
2e-11 km in orbit sizes (Linux vs macOS maths libraries) and one intended difference
below. Building up to 2 November and then adding 3–4 November gives exactly the same
tables as a full rebuild. The pipeline test does the same over two simulated days, plus a
third run with nothing new, which must change nothing.

Size: 274 MB → 78.5 MB published (2.06 M catalogue rows → 71,649 change rows + 71,231
latest rows); growth falls to roughly the new orbit element sets, about 2–3 MB a day.

## Consequences

- History is kept forever, and a run's work no longer grows with the age of the project.
- A day's attribute changes and population counts appear the day after (complete days).
- `fct_reentries` now lists only objects that currently have a decay date. The old model
  also listed an object whose decay date the catalogue later withdrew (RASSVET-3 23), as
  "re-entered" without a date.
- Changing an incremental model needs one full-refresh run. The workflow also falls back
  to a full rebuild by itself when the downloaded warehouse lacks the new tables.
- A bad published warehouse would be continued by the next run. It is only published when
  every dbt test and asset check passes, and a full refresh rebuilds it from the raw
  snapshots, which stay the source of truth.
