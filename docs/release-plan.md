# Zeus release plan

**Status** — Proposed. Versions mark readiness gates. Schedule dates after the first recovery prototype establishes an implementation baseline. The initial deployment target is Linux x86-64 on local SSD storage.

## Release milestones

| Version | Scope | Exit criteria |
| --- | --- | --- |
| 0.1 Embedded alpha | Rust library with byte-string keys and values, get, put, delete, ordered scans, and atomic batches. Use a versioned, checksummed write-ahead log, an in-memory sorted key index, disk-backed values, and manual compaction into an immutable sorted base with an append-only log tail. | Acknowledged batches survive the documented crash model. Recovery exposes no partial batch. Exercise torn log tails, interrupted compaction, corruption, and disk-full behavior. Publish an embedded example and baseline measurements. |
| 0.2 Replicated preview | One Raft group with three fixed members, quorum-durable writes, linearizable reads, snapshot transfer, authenticated connections, and documented retry semantics. | Under leader loss and network partitions, a connected majority can make progress and the minority cannot successfully commit writes or serve stale reads as linearizable. Test restart, retry, follower catch-up, and snapshot installation. Publish a three-node quickstart and the failure model. [Raft paper](https://raft.github.io/raft.pdf) |
| 0.3 Operational beta | Runtime metrics, health checks, backups, restore and certificate rotation procedures, versioned release artifacts, and a documented upgrade policy. | Restore a backup to a fresh cluster, run a supported upgrade, rotate credentials, and verify the published artifacts using the operator documentation. Publish known limits and recovery procedures. |
| 1.0 Stable | Stable documented API and explicit compatibility promises for the wire protocol, data files, and backups. Support one disk-backed deployment profile initially. | Complete a release-candidate soak with sustained writes and compaction. Verify upgrades from the declared supported versions and recovery from an interrupted upgrade. Meet the published performance budgets and complete an independent review of storage recovery and network trust boundaries. |

Sharding, cross-key transactions beyond atomic batches, SQL, and multi-region replication are later decisions. Each replica stores the full dataset in the initial cluster design.

The [replicated preview contract](adr/0006-replication-contract.md) defines the 0.2 client guarantees and failure cases for the acceptance suite.

## Performance and release evidence

The [0.1 alpha checklist](alpha-release.md) records the release gates and benchmark commands.

The [0.2 preview verification](replicated-preview-verification.md) records the clean-copy quickstart, functional tests, and local failover and catch-up timings.
The [0.2 packaging checklist](replicated-preview-release.md) describes the archive, checksum, and prerelease gates.

The [certificate rotation procedure](certificate-rotation.md) covers the offline credential cutover for the operational beta.
The [operational beta upgrade procedure](operational-beta-upgrade.md) covers the preview-to-beta upgrade and pre-write rollback rehearsal.
The [operational beta packaging procedure](operational-beta-release.md) covers the archive, dependency inventory and signed provenance.

Publish hardware, operating system, Rust version, dataset size, key and value sizes, concurrency, and durability settings with each benchmark. Include sustained read, write, and mixed workloads with a dataset larger than RAM. Record throughput, p50 and p99 latency, idle and peak RSS, write amplification, and restart time. For clustered releases, also measure failover time and follower catch-up. Set regression budgets from the alpha baseline before using them as release gates.

Before the first public tag, choose and add an open-source license, pin the Rust toolchain, and document the build from a clean checkout. For every public tag, publish release notes with supported platforms, behavior and compatibility changes, known issues, verification results, and upgrade or restore instructions as applicable. Publish checksums for shipped binaries, and make signed provenance and a dependency inventory part of the beta gate. [SLSA build and provenance specification](https://slsa.dev/spec/v1.2/)

## Documentation cadence

| When | Record |
| --- | --- |
| Before committing to a durable format, API guarantee, or security boundary | Add a short architecture decision record under `docs/adr/` with context, options, decision, consequences, and status. |
| With each behavior change | Update the affected API example, format specification, operator procedure, or security assumption in the same change. Record user-visible changes in the changelog. |
| At each release candidate | Run the quickstart and any affected upgrade and restore procedures from the written instructions. Link release notes to test and benchmark results and include reproduction instructions. |
| When a decision changes | Link to the replacement decision and update the current specification. Preserve the earlier rationale for contributors. |

Start with three decisions covering the acknowledgement and crash model, the disk format and recovery rules, and the client and peer trust boundaries.
