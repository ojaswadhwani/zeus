# Zeus

Zeus is a planned open-source, durable ordered key-value storage engine written in Rust. The proposed design uses one storage core for an embedded library and a lightweight replicated server. The first target is application state that fits on one node's disk and needs to remain available through a single node failure.

- [Release plan](docs/release-plan.md) covers scope, release gates, and documentation.
- [Replication quickstart](docs/replication-quickstart.md) starts the experimental three-node server.
- [Security and AI direction](docs/security-and-ai.md) covers trust boundaries, secure defaults, and integration priorities.

The 0.1 alpha candidate provides a local engine with durable writes, atomic batches, ordered reads, compaction, and checkpoint export. See the [alpha release checklist](docs/alpha-release.md) for verification and benchmark steps.

## Local quickstart

The alpha candidate targets Linux and pins Rust 1.89.0 in `rust-toolchain.toml`.

```sh
cargo test --locked
cargo build --locked
db_dir=$(mktemp -d)
./target/debug/zeus-local "$db_dir/zeus.wal" put checkpoint saved
./target/debug/zeus-local "$db_dir/zeus.wal" get checkpoint
```

The example prints `saved`. The CLI also supports `delete KEY`. The embedded API offers `Store::scan(start, end)` for bytewise ordered scans over a half-open interval and `Store::write_batch` for atomic groups of puts and deletes. See the [write durability decision](docs/adr/0001-write-durability.md) for the recovery contract and format.
The [implementation language decision](docs/adr/0002-implementation-language.md) records the switch to Rust.
Run `zeus-local LOG compact` to reclaim obsolete log records. See the [log compaction decision](docs/adr/0003-log-compaction.md) for disk space and recovery behavior.

Run `zeus-local LOG checkpoint TABLE` to export live data to a new file. The embedded `SortedTable` API supports point reads and ordered scans with disk-backed values. See the [checkpoint format and guarantees](docs/adr/0004-sorted-table-checkpoints.md).
