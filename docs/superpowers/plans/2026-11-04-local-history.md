# Local history implementation plan

Goal: deterministic local processing of out-of-order observations without unbounded replay.
Architecture: pure snapshot state routes work; the application persists state before acknowledgement. Historical reads and classifier are injectable effects. Conflict resolution is application supplied. Annotation overlay preserves operator cuts and applicable ancestry.

- [ ] Test routing nearest predecessor, maxGap boundaries, duplicate observations, local context and eviction with retained work; implement pure state.
- [ ] Test worker reading only local context and durable acknowledgement ordering; implement Cats Effect worker and fs2 pipe.
- [ ] Test applicable annotation inheritance and cuts; implement overlay.
- [ ] Add GitHub CI, MIT license and ownership/persistence contract documentation; run clean sbt test.
- [ ] Push feature branch and open PR. Never merge or release artifacts.
- [ ] Pin review source in supervisor, add adapter coverage, run supervisor quick suite, open MR.
