# Zeus 0.3.0-beta.1

The operational beta targets small three-voter clusters on Linux x86-64. The archive contains the replicated `zeus-node` server, the embedded `zeus-local` CLI, a Rust dependency inventory, and the operator documentation.

- Check authenticated liveness, quorum-backed leader readiness, and per-voter Raft progress.
- Back up the durably applied state of a stopped voter and restore it into a fresh cluster. Backups retain certificate-scoped retry records.
- Upgrade an existing `0.2.0-preview` cluster during a full-cluster restart with the original voter identities. Keep cold copies for rollback before the first mutation request.
- Rotate the CA and client and voter certificates during a maintenance window using backup and restore.

The snapshot image and restore are limited to 16 MiB including metadata. Snapshot compaction pauses above the limit. Voter membership is fixed, and the upgrade and rotation procedures require downtime. Performance evidence is limited to small-state tests on local hosts. The binaries are dynamically linked and the release workflow builds on Ubuntu 24.04.

The release workflow runs the optimized tests and rehearses the preview upgrade and rollback using the extracted server. It verifies the archive and signed build provenance before publishing.

- [Verify the package](https://github.com/ojaswadhwani/zeus/blob/v0.3.0-beta.1/docs/operational-beta-release.md)
- [Upgrade and roll back](https://github.com/ojaswadhwani/zeus/blob/v0.3.0-beta.1/docs/operational-beta-upgrade.md)
- [Back up and restore](https://github.com/ojaswadhwani/zeus/blob/v0.3.0-beta.1/docs/backup-restore.md)
- [Rotate certificates](https://github.com/ojaswadhwani/zeus/blob/v0.3.0-beta.1/docs/certificate-rotation.md)
