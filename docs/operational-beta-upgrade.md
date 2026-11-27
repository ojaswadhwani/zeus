# Upgrade a replicated preview cluster to the operational beta

The 0.3 upgrade path starts from a healthy Linux x86-64 cluster running `0.2.0-preview` with three fixed voters. The rehearsal uses a full-cluster stop and reopens the existing voter directories with the candidate binary. Retain the voter IDs, peer addresses, CA, and certificate files. Schedule [certificate rotation](certificate-rotation.md) separately.

Application writes pause before shutdown. The cluster is unavailable from shutdown through the replacement election and read-only verification. Reserve space for a complete cold copy of all three voter directories.

## Prepare and stop the preview

1. Retain the preview binary and each voter's startup configuration. Stage a verified operational-beta binary at a separate path on each host.
2. Pause application writers and resolve outstanding requests using their existing certificate, sequence, and command. Use a successful read to identify the preview leader. Record a key and expected value, a known deletion, and the last confirmed command and sequence for a client you can use for verification.
3. Stop all three voters and wait for the processes to exit. Copy each entire voter directory, including the Raft log, state machine, and snapshot sidecars. Keep these copies separate from the directories the beta will open. For example, on the host for voter 1, with `rollback_dir` set to an existing separate backup directory:

   ```sh
   cp -a -- node1 "$rollback_dir/"
   sync -f "$rollback_dir"
   ```

   Use a new rollback location for each attempt and confirm that all three copies completed before starting the beta.

## Start and verify the beta

Start all three voters with the beta binary and the same `serve` arguments and working directories. Keep the writers paused. Use [the health commands](replication-quickstart.md#check-health-and-raft-progress) to confirm liveness on all voters and readiness on the elected leader. Confirm the expected value and deletion with `get`. Check `metrics` on all voters for applied progress.

If you decide to proceed, retry the last recorded command with its original certificate and sequence and check that it returns `OK`. Verify that any later update to the same key is preserved. Submit a new write at the next sequence number and read it back. Continue client sequences from the confirmed writes and resume application traffic.

## Roll back before writes

The rollback window closes before the first mutation request sent to the beta, including the verification retry. If a startup or read-only check fails, keep writers paused, stop all beta voters, and preserve their directories for diagnosis. Put the three untouched cold copies into service with the preview binary and the original startup configuration. Verify the expected value and deletion before reopening traffic.

Once the beta accepts writes, restarting from the cold copies would lose those writes. Recover with the beta binary and reconcile the affected operations before attempting any downgrade.

## Rehearse the upgrade

From the candidate checkout, build the tagged source with the pinned Rust toolchain and run the explicit two-version test. The test uses loopback sockets and OpenSSL.

```sh
preview_source="$(mktemp -d)"
git archive v0.2.0-preview | tar -x -C "$preview_source"
preview_target="$(pwd)/target/upgrade-preview"
cargo build --locked --release --features replication --bin zeus-node \
  --manifest-path "$preview_source/Cargo.toml" --target-dir "$preview_target"
ZEUS_PREVIEW_BIN="$preview_target/release/zeus-node" \
  cargo test --locked --release --features replication --test node_server \
  offline_preview_upgrade_preserves_state_and_allows_prewrite_rollback -- --ignored
```

The rehearsal creates a preview snapshot and committed log suffix, opens the original directories with the candidate, and checks that the preview can still run from the cold copies before candidate writes. It then verifies a retry from before the upgrade, preservation of a later overwrite and a deletion, and a new candidate write.
