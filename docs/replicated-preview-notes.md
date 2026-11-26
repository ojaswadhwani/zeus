Zeus 0.2.0-preview adds a replicated key-value server for small-state clusters on Linux x86-64.

- Three fixed Raft voters use mutual TLS, elect a leader automatically, and acknowledge writes after quorum durability.
- `zeus-node` supports linearizable point reads and bounded scans, atomic put/delete batches, and retries scoped to a client certificate and sequence number.
- Lagging followers catch up through chunked snapshot transfer. Snapshot images are limited to 16 MiB, including retry records and metadata. Above that limit, voters retain their previous snapshots and Raft log compaction pauses.

The release archive includes both `zeus-node` and the embedded `zeus-local` CLI. Verify it with `sha256sum -c SHA256SUMS`. Start replicated voters in fresh data directories. Keep embedded 0.1 databases separate.

The candidate passed 21 optimized tests, including network partitions, leader loss, interrupted snapshot transfer and installation, and lost batch replies. Local loopback timings from five optimized trials had medians of 1.61 s for failover and 206 ms for follower catch-up. Larger-than-RAM sustained workloads and remote-network performance remain unverified.

- [Three-node quickstart](https://github.com/ojaswadhwani/zeus/blob/v0.2.0-preview/docs/replication-quickstart.md)
- [Failure model and replication contract](https://github.com/ojaswadhwani/zeus/blob/v0.2.0-preview/docs/adr/0006-replication-contract.md)
- [Verification results](https://github.com/ojaswadhwani/zeus/blob/v0.2.0-preview/docs/replicated-preview-verification.md)
