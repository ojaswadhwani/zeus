# Zeus 0.2 replicated preview verification

Verified locally on 2026-11-27. The clean source copy contained the current candidate changes over base commit `acf7ff8`, including files that had yet to be committed. It started without a build tree or data directories and used the pinned Rust toolchain and cached dependencies from the lockfile. The SHA-256 of the build-input manifest was `5b632074d912905bdec3fd29f6030fdb3aaec7c2833c2924aba8d4a849166426`. The manifest hashes each Rust source file, `Cargo.toml`, `Cargo.lock`, and `rust-toolchain.toml` by bytewise sorted relative path, with each path followed by a NUL byte and the raw 32-byte file digest.

## Functional verification

- The [three-node quickstart](replication-quickstart.md) built from the clean copy, created fresh TLS credentials, acknowledged a put, and returned `saved` from the automatically elected replacement after SIGKILL. The subsequent mixed batch returned `OK`, and the scan returned the expected `status` row with value `ready`.
- `cargo test --locked --release --all-features --all-targets` passed 21 tests. The worker entry point was invoked by its parent process test. The suite covered majority progress, minority rejection, conflicting log repair, loss of a batch reply, interrupted snapshot transfer and installation, and peer identity checks.
- The [replication contract](adr/0006-replication-contract.md) documents the failure model and acceptance cases. The verified cluster consists of three fixed trusted voters, with any two forming a quorum. The tests exercise process crashes and network partitions. Durability assumes that local storage honors sync.

## Failover and follower catch-up

Five serial, optimized runs used one Intel Core i7-12700H host with 20 logical CPUs and about 14.8 GiB RAM. The host ran Ubuntu 26.04.1 LTS on Linux 7.0.0-44-generic x86-64 with ext4 over local NVMe. The toolchain was Rust 1.89.0 and credentials were generated with OpenSSL 3.5.5. Each run used three local processes with fresh stores and mutual TLS over loopback. One client made sequential writes with quorum durability. The workload wrote 32 distinct 12 KiB values, with short initial and suffix values and keys between 3 and 7 bytes for that portion of the test.

| Trial | Failover | Snapshot catch-up |
| --- | ---: | ---: |
| 1 | 3,443 ms | 319 ms |
| 2 | 1,487 ms | 318 ms |
| 3 | 2,000 ms | 206 ms |
| 4 | 1,378 ms | 195 ms |
| 5 | 1,611 ms | 183 ms |
| Median | 1,611 ms | 206 ms |

Failover runs from sending SIGKILL to the first confirmed client read on the replacement leader. Snapshot catch-up runs from resuming the leader after an interrupted transfer to observing snapshot activation and the committed suffix in the follower's state-machine file. Both include polling delays. These samples describe a small loopback cluster on one host.

Run the timings again with the data directories on the intended filesystem.

```sh
TMPDIR=/path/to/benchmark-directory cargo test --locked --release \
  --features replication --test node_server \
  authenticated_three_node_cluster_elects_replacement_after_sigkill -- --nocapture
```

## Remaining release gates

This verifies the small-state preview on Linux x86-64. The 16 MiB snapshot image limit includes retry records and metadata, and nodes stop creating new snapshots and compacting the Raft log above it. The [release plan](release-plan.md) calls for sustained workloads with data larger than RAM. That evidence, including remote-network measurements and behavior beyond the tested small-state profile, remains open. The preview requires fresh voter directories. An in-place migration from the 0.1 embedded database is outside the current preview scope.

Candidate release notes are in the [changelog](../CHANGELOG.md). The verification applies to this local candidate.

## Versioned archive check

The `0.2.0-preview` candidate was rebuilt from a clean source copy and passed all 21 optimized tests. Its build-input manifest SHA-256 is `ee51fbc58c351ace717d064e2cface9758fe2f6a2fa2d0a29aa4311832810101`. The tests ran with data under a long filesystem path after separating the worker socket paths from the test data directory.

The local Linux archive passed its SHA-256 check. Both extracted binaries reported `0.2.0-preview`. The embedded CLI round-tripped a value, and the extracted server passed the three-node quickstart through leader replacement, a mixed batch, and a bounded scan. The local `zeus-node` binary references glibc symbols through version 2.34. The Ubuntu 24.04 workflow build remains the distribution check. See the [packaging checklist](replicated-preview-release.md).
