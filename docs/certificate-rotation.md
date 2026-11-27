# Rotate cluster certificates

Rotate the cluster CA and all voter and client certificates by restoring the cluster into fresh voter directories. The replacement voters use only the new CA for mutual TLS. This retires access for certificates signed solely by the old CA. Replacing just the voter leaf certificates under the old CA would leave the retired dual-purpose voter certificates eligible for client access.

Plan a maintenance window. Application writes pause before the backup and resume after cutover. Service downtime starts when the old voters stop and lasts through backup, restore, election, and verification. The applied state and retry records must fit within the 16 MiB snapshot limit, including space for voter identities. See the [backup size and recovery point](backup-restore.md#take-a-backup).

## Prepare before the window

1. Create a separate replacement working directory. Use the [credential setup in the quickstart](replication-quickstart.md#run-three-zeus-voters-locally) to issue a new CA and new certificates and keys for all three voter IDs. Issue client certificates from that CA and stage the new trust bundle and client credentials for cutover.
2. Prepare the replacement `serve` commands with the new CA, leaf certificates, and keys. Each voter must pin the new leaf certificate of each peer. Use the same certificate files when running `restore`.
3. Record a key and expected value and a key known to be missing. Keep the old working directories, credentials, and startup configuration available for rollback.

## Stop, back up, and restore

1. Pause application writers and resolve outstanding writes with their original credentials and sequence numbers. Use `zeus-node ready` to identify the current leader and confirm that it can serve a quorum-backed request.
2. Stop all three old voters. Back up the stopped leader. For example, from the old working directory, with voter 1 as the stopped leader and an absolute replacement directory path in `replacement`:

   ```sh
   "$zeus_node" backup 1 node1 "$replacement/zeus-backup.table"
   ```

3. In the replacement directory, seed all three empty voter directories. Start the replacement voters only after all three restores succeed. Keep the old cluster stopped.

   ```sh
   cd "$replacement"
   mkdir node1 node2 node3
   for id in 1 2 3; do
     "$zeus_node" restore "$id" "node$id" zeus-backup.table \
       node1.pem node2.pem node3.pem || exit 1
   done
   ```

## Verify and cut over

Set `new_leader` to the replacement leader's address. With the new client credentials, check readiness, read the expected value, and confirm that the missing key returns `MISSING`.

```sh
"$zeus_node" ready "$new_leader" ca.pem client.pem client.key
"$zeus_node" get "$new_leader" ca.pem client.pem client.key "$expected_key"
"$zeus_node" get "$new_leader" ca.pem client.pem client.key "$missing_key"
```

To check retirement, connect using the new CA to verify the server and present an old leaf certificate and its corresponding key. The command must fail during mutual TLS and return no `OK`. Check both a retired voter and a retired client credential.

```sh
"$zeus_node" live "$new_leader" ca.pem "$old_dir/node1.pem" "$old_dir/node1.key"
"$zeus_node" live "$new_leader" ca.pem "$old_dir/client.pem" "$old_dir/client.key"
```

Before allowing the first write on the replacement, you can roll back by stopping the replacement voters and restarting the old voters with their original directories and credentials. Restore the old client configuration before unpausing traffic. Once the replacement accepts writes, returning to the old directories would discard those writes.

After the read-only checks pass and you choose to cut over, set `probe_key` to a key reserved for maintenance checks. Verify that the write returns `OK` and the read returns `rotated`. Each new client certificate starts at sequence 1. If you use an application certificate for the verification write, continue that certificate at sequence 2. Then resume application traffic with the new endpoints and credentials.

```sh
"$zeus_node" put "$new_leader" ca.pem client.pem client.key 1 "$probe_key" rotated
"$zeus_node" get "$new_leader" ca.pem client.pem client.key "$probe_key"
```
