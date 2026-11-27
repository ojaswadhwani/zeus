# Back up and restore a replicated cluster

Use a build of `zeus-node` that includes the `backup` and `restore` commands. The backup captures the selected voter's last durably applied state, including client retry records. A stopped follower can lag the leader, and committed entries that the source voter had yet to apply are outside the recovery point.

## Take a backup

For a planned recovery point, pause client writes, resolve outstanding write results, and confirm a successful read on the current leader. Stop that leader and keep it stopped through the backup. Use its voter ID and data directory. If the source is an available stopped follower after an incident, account for possible lag before resuming writes against the restored cluster.

```sh
# Example using voter 1 as the stopped source.
"$zeus_node" backup 1 node1 zeus-backup.table
```

The command takes exclusive store locks and publishes a checksummed file at a new path. Store the file as sensitive data. It contains values and client retry records in plaintext. The state must fit within the 16 MiB snapshot image limit, with up to 12,300 bytes reserved for destination voter identities. Keep a copy of the original backup file for each restore attempt.

## Restore all three voters

Stop and isolate the old cluster before directing clients to the replacement. Set up the destination TLS credentials as in the [three-node quickstart](replication-quickstart.md). Run the following commands in a destination working directory with the backup file and the destination certificates. Supply the three voter certificates in voter ID order on every invocation.

```sh
mkdir node1 node2 node3
for id in 1 2 3; do
  "$zeus_node" restore "$id" "node$id" zeus-backup.table \
    node1.pem node2.pem node3.pem || exit 1
done
```

Start the voters with the quickstart's `serve` commands only after all three restore commands succeed. Use the same backup and certificate list for all three voters. If a restore attempt fails, use a fresh set of directories for a new attempt.

Verify an expected key and an expected deletion on the new leader, then submit a new write and read it back before redirecting traffic. A client using the same trusted leaf certificate continues from the sequence saved in the backup. Reconcile writes after the recovery point before resuming that sequence. Clients using a new certificate start at sequence 1.
