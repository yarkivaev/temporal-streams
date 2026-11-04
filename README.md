# Temporal streams

Local historical stream processing for out-of-order observations. Scala 3, Cats Effect and fs2.

Implementation is proposed through pull requests; no artifact release is performed by CI.

## Contract

`LocalStreams` is an immutable snapshot of cursor ownership and pending work. Persist the snapshot atomically with input acceptance before acknowledging upstream. Cursor eviction never removes pending work. Recover pending work from the snapshot after a crash; call `acknowledge` only after idempotent output commit. Applications must bound snapshot size by processing/compacting pending work and retaining only the required history.

A point goes to the nearest cursor at or before its event time within `maxGap` (inclusive). Otherwise a new cursor is created. Wall-clock `idleTimeout` is independent from event-time gaps. `merge` requires equal positions and an explicit complete-state equivalence check by the classifier. Cursor merge never merges timeline segments.

`History` is a reference in-memory history implementation. It groups timestamps and channels, rejects replayed event IDs, and accepts an application `Conflict` strategy. `Conflict.first` preserves the first accepted channel value at an exact timestamp. Production stores must enforce this atomically. Neighbour queries include one group before and after the local range, never an automatic replay to the present.

`Worker` consumes durable `Work` through injected `Processing` ports. The classifier can read additional bounded context through its application adapter when the initial neighbours are insufficient. `None` context skips classification, as for observations older than retained source history. Commit failure preserves work for retry. Output commits must remain idempotent if a process dies after commit but before acknowledgement.

`Overlay` reapplies annotations to applicable segment kinds through explicit ancestry, including extended segment boundaries, preserves user cuts, and never merges segments. The application owns durable segment identities and ancestry. Source observations, user actions and calculated segments are separate persistence concerns.

Run `sbt test`. CI only builds and tests; it never publishes packages or deploys applications.
