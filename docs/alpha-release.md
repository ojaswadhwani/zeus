# Zeus 0.1 alpha release checklist

The alpha targets Linux x86-64 on local SSD storage and ships a source library with the `zeus-local` example. Confirm the following gates before creating the first public tag.

- From a clean checkout, run `cargo test --locked --all-targets` and `cargo build --locked --release --bins --examples` with pinned Rust. Run the README quickstart using the release binary.
- Verify the Linux CI workflow, including process-crash recovery, truncated or corrupted records, failed appends and compaction under a file-size limit, and actual disk exhaustion on a bounded filesystem. Record the release commit and CI results.
- Run the benchmark below on the intended deployment filesystem. Publish the raw results and environment details with the release notes.
- Enable private vulnerability reporting and assign a triage maintainer. Verify the source license, and include the license, supported platform, known limits, and format compatibility notes in the release. Publish a SHA-256 checksum for any binary included.
- Mark the changelog entry released and publish only after the evidence is recorded.

## CI artifacts

Run the **Embedded alpha checks** workflow manually to capture a 256 MiB hosted baseline under a 128 MiB cgroup limit with swap disabled. The job checks the memory limit, records the runner details and benchmark output in `baseline.txt`, and uploads them with the binary archive. The hosted baseline describes that runner. Use the deployment filesystem for release performance measurements.

A successful CI run uploads the Linux x86-64 archive and checks its `SHA256SUMS` file. After downloading and unpacking the workflow artifact, run `sha256sum -c SHA256SUMS` in that directory. CI artifacts are candidates until the release gates pass.

## Reproduce the benchmark

Build the release example and choose a fresh path on the target filesystem. The benchmark creates that path and leaves the database for inspection. During compaction the filesystem must have room for the original database and the replacement. Use a payload larger than available RAM or run inside a Linux memory cgroup with a limit smaller than the payload and sufficient space for the key index.

```sh
rustc --version
uname -srvmo
lscpu
findmnt -T /path/to/benchmark-directory
cargo build --locked --release --example baseline
/usr/bin/time -v target/release/examples/baseline \
  /path/to/benchmark-directory/baseline.wal 131072 65536
```

The example above writes 8 GiB of values. Scale it to the test machine's effective RAM and disk budget. Record available RAM or the cgroup limit, device type, filesystem, and mount options. Use a fresh path for each run and record cache conditions. The workload uses a single thread, sequential inserts, deterministic pseudo-random point reads, and a 90 percent read and 10 percent overwrite mix. Each write is synced. Values are repeated bytes, so record filesystem compression or deduplication settings. The amplification estimate counts logical file bytes written, including compaction. Device-level amplification can differ.

The program reports throughput, sampled p50 and p99 latency, process RSS and high-water RSS, compaction time, and reopen times before and after compaction. Archive the output with the release. A CI smoke run uses a small dataset to verify the benchmark entry point.

## Evidence status

The verification steps and benchmark are prepared. Build, CI, full-size benchmark results, and a public tag are pending.
