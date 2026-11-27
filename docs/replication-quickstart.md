# Run three Zeus voters locally

For builds that include the offline commands, see [Back up and restore a replicated cluster](backup-restore.md).
For a planned change of voter and client credentials, see [Rotate cluster certificates](certificate-rotation.md).

Build the experimental server on Linux with the pinned Rust toolchain. Use OpenSSL to issue credentials for this local cluster.

If you have the [beta archive](operational-beta-release.md) or [preview archive](replicated-preview-release.md), verify and extract it, then substitute `zeus_node="$(pwd)/zeus-node"` from inside the extracted directory for the build and binary path lines below.

```sh
cargo build --locked --features replication --bin zeus-node
zeus_node="$(pwd)/target/debug/zeus-node"
workdir="$(mktemp -d)"
cd "$workdir"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -days 1 -subj '/CN=Zeus local CA' -addext 'basicConstraints=critical,CA:TRUE' \
  -keyout ca.key -out ca.pem
for name in node1 node2 node3 client; do
  usage=serverAuth,clientAuth
  if [ "$name" = client ]; then usage=clientAuth; fi
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$name" -keyout "$name.key" -out "$name.csr"
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=%s\nsubjectAltName=DNS:zeus.local\n' \
    "$usage" > extensions
  openssl x509 -req -days 1 -CA ca.pem -CAkey ca.key -CAcreateserial \
    -in "$name.csr" -out "$name.pem" -extfile extensions
done
```

Issue one distinct dual-purpose TLS certificate for each voter. The configured leaf certificates bind voter IDs to mutual TLS identities, and all server certificates must include the DNS name `zeus.local`. Use a dedicated cluster CA. Client certificates signed by that CA grant access to read and write keys. The server scopes each client's retry sequence to its leaf certificate, so retry with the same certificate after an uncertain outcome. A replacement certificate starts a new sequence.

Start the three processes from the temporary directory. Use Bash for the PID array.

```bash
mkdir node1 node2 node3
pids=()
for id in 1 2 3; do
  peers=()
  for peer in 1 2 3; do
    if [ "$id" != "$peer" ]; then
      peers+=("$peer=127.0.0.1:740$peer,node$peer.pem")
    fi
  done
  "$zeus_node" serve "$id" "node$id" "127.0.0.1:740$id" \
    ca.pem "node$id.pem" "node$id.key" "${peers[@]}" &
  pids[$id]=$!
done
```

Send a write with sequence 1. If the node returns `NOT_LEADER`, use the port for the reported voter ID. A hint of 0 means the node has no known leader. Retry timed-out writes with the original sequence, key, and value. New writes advance the sequence by one.

```bash
# Set leader to the current voter ID.
leader=1
"$zeus_node" put "127.0.0.1:740$leader" ca.pem client.pem client.key 1 status saved
```

Once the command prints `OK`, stop that process with `kill -KILL "${pids[$leader]}"`. Try the surviving ports until a read returns `saved`. The replacement is elected automatically.

```bash
# Set replacement to a surviving voter ID.
replacement=2
"$zeus_node" get "127.0.0.1:740$replacement" ca.pem client.pem client.key status
```

Apply a batch using the next sequence number. The operations apply in the order given, and a retry uses the same sequence and operation list.

```bash
"$zeus_node" batch "127.0.0.1:740$replacement" ca.pem client.pem client.key \
  2 put status ready delete old-status
"$zeus_node" scan "127.0.0.1:740$replacement" ca.pem client.pem client.key a z 10
```

A scan reads the half-open byte range `[START, END)` from one applied state after fresh quorum confirmation. The CLI prints escaped key and value bytes separated by a tab. It writes `MORE` to stderr if the requested row limit or the 96 KiB page limit leaves additional keys in the range.

Stop the surviving servers with `kill` followed by their stored PIDs. Retain the temporary directory if you want to reopen their data files.

The server accepts one active client operation per leader and gives accepted requests a two-second deadline. Keys can be up to 1 KiB and values up to 16 KiB. A replicated batch can contain up to 128 operations and 64 KiB of encoded operations. A scan can request up to 128 rows.

Every 32 applied Raft entries, a voter attempts to create a snapshot and compact its covered Raft log. The snapshot includes retry state and is bound to the configured voter certificates. Transfers use 64 KiB chunks over mutual TLS. If a connection breaks, the next transfer starts from the first chunk. A voter validates the complete image before recording it on disk and installing it.

The current implementation assembles the image in memory and caps it at 16 MiB. If the next image exceeds that limit, the voter retains its prior snapshot and the Raft log continues to grow.

## Check health and Raft progress

Build a version of `zeus-node` with the health commands and use the same client credentials as for reads and writes.

```sh
"$zeus_node" live 127.0.0.1:7401 ca.pem client.pem client.key
"$zeus_node" ready 127.0.0.1:7401 ca.pem client.pem client.key
"$zeus_node" metrics 127.0.0.1:7401 ca.pem client.pem client.key
```

`live` prints `OK` when the authenticated request reaches the voter's event loop. It can succeed on an isolated voter. `ready` prints `OK` only after the leader confirms a fresh quorum and applies through the confirmed index. Probe the leader or follow the `NOT_LEADER` hint to another voter. A `BUSY` response means the probe could not enter the active request slot. Retry it on the next polling interval. Both checks exit with status 0 on success, 2 on server rejection, or 1 on a connection or other client error.

`metrics` returns local values as one `key=value` pair per line and works on followers and isolated voters.

| Field | Meaning |
| --- | --- |
| `voter_id`, `role`, `term`, `leader_id` | The responding voter and its local view of Raft. A leader ID of 0 means the voter has no known leader. |
| `commit_index`, `applied_index` | The voter's known commit index and progress applying entries. Compare applied indexes across voters to see catch-up progress. |
| `snapshot_index` | The latest snapshot created or installed on this voter. A value of 0 means it has no snapshot. |
| `snapshot_limit_blocked` | A value of 1 means the latest attempted snapshot exceeded the 16 MiB limit. The value clears on successful snapshot creation or installation and starts at 0 after a restart. |
