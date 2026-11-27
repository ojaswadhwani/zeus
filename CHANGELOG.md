# Changelog

## 0.3.0-beta.1

- Added offline backup of a stopped replicated voter and restore into a fresh three-voter cluster. Restores preserve key-value data and certificate-scoped retry records. See the [backup and restore procedure](docs/backup-restore.md).
- Added authenticated liveness and quorum-backed readiness checks, plus per-voter Raft progress and snapshot metrics.
- Documented and tested offline rotation of the cluster CA and voter and client certificates.
- Documented and rehearsed the offline upgrade from `0.2.0-preview`, including rollback from cold copies before candidate writes.
- Added Linux x86-64 beta packaging with checksums, a Rust dependency inventory, and a hosted signed-provenance gate.

## 0.2.0-preview (2026-11-27)

- Added the opt-in `zeus-node` server with three fixed voters, automatic elections, mutually authenticated peer connections, and quorum-durable writes.
- Added point reads and bounded scans with linearizable semantics, atomic put/delete batches, and retries scoped to the client certificate and sequence number.
- Added durable snapshot installation, Raft log compaction, and catch-up over mutual TLS. Snapshot images are capped at 16 MiB. Above that cap, new snapshots and compaction pause.
- The replicated preview targets Linux x86-64 with fresh data directories. The embedded 0.1 database has no in-place migration to a replicated cluster.
- See the [three-node quickstart](docs/replication-quickstart.md) and [candidate verification results](docs/replicated-preview-verification.md).

## 0.1.0 alpha (2026-11-25)

- Embedded Rust library with durable puts and deletes, atomic write batches, and ordered scans.
- Disk-backed values, an in-memory key index, and manual compaction into a sorted base with a write-ahead log tail.
- Read-only sorted checkpoint export and an example CLI for local operation.
- Existing `ZWL1`, `ZWD1`, and `ZWB1` logs remain readable. A compacted database starts with `ZEUSDB01` and requires a build that supports that format.
- Point reads and range scans return I/O errors and owned values because values are fetched from disk.
- Linux x86-64 is the initial release target. Embedded compaction is synchronous and needs temporary space for a copy of live values.
