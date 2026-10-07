# ADR 0001: Build our own history from snapshots

- Status: accepted
- Date: 2026-10-07

## Context

CelesTrak publishes the *current* orbital elements (GP data) for every tracked object and
the current satellite catalogue (SATCAT), free and without an API key. It does not serve
history. Historical element sets exist at Space-Track.org, which needs an account and
has stricter terms.

Questions such as "which satellites manoeuvred this week?", "when did this object's status
change?" or "how fast is the debris population growing?" need history.

## Decision

- Take snapshots on a schedule (every 6 hours with Dagster; once a day on GitHub Actions)
  and **keep every snapshot**: raw bytes (gzip) in `data/raw`, typed Parquet in
  `data/bronze`, partitioned by snapshot date.
- Treat bronze as immutable. Everything downstream (dbt models) is rebuilt from it.
- Publish each day's bronze files as a GitHub Release, so the archive survives the
  ephemeral CI runner and anyone can rebuild the warehouse from it.

## Consequences

- History starts on the day the project started (7 October 2026, UTC) and grows daily.
- The SCD type 2 dimension and orbit-change facts are computed from snapshots with SQL
  (ADR 0002), so they are correct no matter when or how often the pipeline ran.
- Storage grows by a few MB a day. The collect job restores the last 90 days; a real
  deployment would use object storage and compact old partitions.
