# ADR 0003: Close-approach screening with a KD-tree and linear refinement

- Status: accepted
- Date: 2026-10-07

## Context

Checking every pair of ~20,000 objects is 200 million pairs per time step, and a day has
thousands of steps. Objects in low orbit move at ~7.5 km/s, so two of them can close
distance at up to ~15.5 km/s.

## Decision

1. Propagate all objects with SGP4 (vectorised C implementation) on a 20-second grid,
   in chunks of time steps to bound memory.
2. At each step, build a KD-tree over positions and query pairs within
   `threshold + 16 km/s x step / 2`. Nothing outside that radius can come within the
   threshold during that half-step either side.
3. For each candidate, assume straight-line relative motion within the step and solve
   for the closest approach analytically: `t* = -(dr . dv) / |dv|^2`, clamped to the step.
4. Keep each pair's minimum.
5. Exclude pairs moving together (relative speed < 10 m/s): docked vehicles and formation
   flyers are always "close" but are not conjunctions. Exclude element sets older than
   14 days, which propagate with errors of 100+ km.

## Verification

A test compares the result with a 1-second brute-force propagation for two crossing
orbits: they agree within 50 m.

## Limitations

GP elements + SGP4 are accurate to about 1 km at epoch, degrading by a few km per day,
and carry no covariance. Results are screening candidates for exploring the data, not
collision probabilities. Operators rely on conjunction data messages from Space-Track.
