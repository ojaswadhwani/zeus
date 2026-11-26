# Packaging Zeus 0.2.0-preview

This prerelease targets small-state clusters on Linux x86-64. The [verification report](replicated-preview-verification.md) describes the workload and timing evidence. The [three-node quickstart](replication-quickstart.md) lists the operating limits and runtime requirements.

## Build and check the archive

Start from the candidate source with the pinned Rust toolchain. Put test data on the filesystem being verified.

```sh
cargo test --locked --release --all-features --all-targets
bash scripts/package-preview.sh
cd target/preview
sha256sum -c SHA256SUMS
tar -xzf zeus-0.2.0-preview-linux-x86_64.tar.gz
cd zeus-0.2.0-preview-linux-x86_64
./zeus-node --version
./zeus-local --version
```

The archive contains both binaries, the MIT license, security reporting instructions, release notes, and documentation. Run the quickstart with the extracted `zeus-node` and fresh data directories. The Linux binaries are dynamically linked. Use the Ubuntu 24.04 workflow artifact for distribution and verify it on the intended host before use.

## Prepare the prerelease

- Review the candidate source and confirm the small-state scope. The snapshot image limit is 16 MiB, including retry records and metadata. Compaction pauses when the next image exceeds the limit. Larger-than-RAM sustained workloads and remote-network performance still need evidence.
- Use the **Replicated preview package** workflow on the approved source. A manual run can produce a candidate artifact. For publication, use the run for the matching `v0.2.0-preview` tag so the tested source and release archive correspond to the same commit.
- The `v0.2.0-preview` workflow verifies `SHA256SUMS` and checks the extracted binaries before attaching the archive and checksum file to the GitHub prerelease with the [release notes](replicated-preview-notes.md).
- Start the replicated preview in fresh voter directories. Keep embedded 0.1 data separately. The existing embedded CLI remains available in the preview archive.

Manual workflow runs upload an artifact for review. The matching release tag also enables the publication job after the tested archive is available.
