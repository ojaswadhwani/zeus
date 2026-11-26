# Replicated preview contract

**Status** — Proposed for 0.2.

## Membership and failure model

One Raft group has three fixed voting members. A quorum is any two members. The cluster assumes trusted nodes, local storage that honors sync, and crash-recovery failures. Messages may be delayed, dropped, duplicated, or reordered. A connected quorum can resume progress once it elects a leader and communication stabilizes. Loss of a quorum prevents fresh commits and linearizable reads.

Peer connections use mutual TLS and bind the authenticated identity to a configured member of the same cluster. Client permissions follow the [network trust model](../security-and-ai.md). A node encountering an unrecoverable persistence error stops participating until it can reopen and recover its durable state.

## Writes and retries

Each mutation, including an atomic batch, occupies one Raft log entry. Persist term and vote before sending responses that depend on them. A peer acknowledges an append only after the entry and any required log changes are durable. The leader follows Raft's current-term commit rule and acknowledges a client mutation after quorum durability and local application in log order. [Raft paper, sections 5.3 and 5.4](https://raft.github.io/raft.pdf)

Clients serialize mutations per identity and give each new mutation the next sequence number, starting at one. The identity is scoped to the authenticated principal. After a timeout or leader change, the client retries the same identity, sequence, and command because the original outcome may be unknown. Store the latest sequence, command fingerprint, and original result in replicated state. An identical retry of the latest command returns that result without applying the mutation again. Reusing that sequence with a different command, sending an older sequence, or skipping a sequence produces an error. Keep the retry record across restart and snapshot installation. The preview retains one record per identity for the lifetime of the cluster.

## Reads

The leader commits an entry in its current term before serving reads. For each read, it confirms leadership with fresh quorum responses associated with that request, captures a committed index, and waits for the state machine to apply through that index before reading. Evaluate a point read or bounded scan against one applied state. A follower may return a leader hint. Without fresh quorum confirmation, the request waits until its deadline or returns a retryable error. [Raft paper, section 8](https://raft.github.io/raft.pdf)

## Storage and snapshots

The Raft log and durable term and vote belong to the replication layer. Apply only committed entries to the embedded store, using its atomic batch semantics. The state machine must recover user data, retry metadata, and its applied index at the same log boundary. If persisted independently, recover by rebuilding the complete state machine from an installed snapshot and replaying committed entries before serving. Keep replication metadata outside the client keyspace. The existing [embedded batch format](0001-write-durability.md) bounds the size and operation count of each applied batch.

A snapshot pairs a consistent image of the state machine with its last included index and term, cluster identity, and retry records. The embedded [checkpoint export](0004-sorted-table-checkpoints.md) can represent user data. Installation must validate and durably activate the complete snapshot before acknowledging it or discarding the covered log prefix. Recovery after interruption must select a complete previous or replacement state and apply any committed suffix in order.

## Acceptance cases

Drive delivery, partitions, and restart points explicitly in a three-member test harness. Use fresh node directories at setup and reopen the same directories on restart. Assert results through the client interface and observe persistence boundaries through test hooks.

Run the replication prototype with `cargo test --locked --test replication`. It drives raft-rs messages in process and uses separate Zeus stores for the Raft log and applied data. The quorum-durability test drops the leader before its commit notification reaches either follower. The surviving voters reopen their stores, elect a replacement, and read the acknowledged value through the harness client. The read test uses raft-rs ReadIndex to cover a confirmed read waiting for local application and a fresh read staying pending on an isolated former leader. The harness covers one in-process point read at a time.

The lost-reply test restarts the surviving voters before retrying a batch on the new leader. The harness commits the batch, retry record, and applied index in one embedded write. It stores the exact bytes of each client's latest accepted command and returns the original success for an identical retry. The harness uses numeric client identities and small put-only batches.

The application restart tests reopen a replica after a durable Raft commit, with either no state-machine append or an incomplete batch record generated by the embedded store. They verify that recovery retains the previous applied state until it replays the complete entry. A quorum-confirmed read waits for replay, and an identical retry preserves a subsequent overwrite.

Run the three-process harness test with `cargo test --locked --test replication three_process_failover`. It relays serialized Raft messages over Unix sockets and explicitly triggers elections.

Run `cargo test --locked --features replication --test node_server` to exercise the standalone server with mutually authenticated TCP connections and timer-driven elections. The test checks leader hints and rejects frames from unconfigured and mismatched peer identities. It then kills the leader after an acknowledged write and reads and retries the write on the automatically elected replacement. The [replication quickstart](../replication-quickstart.md) documents how to run the server and its current limits.

The standalone test also drops the connection before reading a mixed put/delete batch reply. After failover, it overwrites the affected keys using a second client certificate and retries the original batch. A bounded scan confirms that the retry preserves both later updates. The in-process read test pauses state-machine application after quorum confirmation and verifies that a scan waits for its applied-index barrier.

The standalone partition test blocks peer TCP connections to and from an elected leader while leaving client connections open. It verifies that the isolated voter appends a conflicting write without applying or acknowledging it, while the surviving majority elects a replacement and serves the new value. After healing, the test reopens the former leader's stores and verifies that the majority's entry replaced the conflicting suffix and the new value reached its applied state.

The snapshot tests cover two interruption points. The standalone test disconnects after the first 64 KiB chunk of an authenticated multi-chunk transfer. The receiver leaves its previous state active and installs the full image when the leader retries. The installation recovery test interrupts after the Raft log durably references the checksummed snapshot file. On reopen, the receiver rebuilds the staged state-machine file, syncs and activates it, then replays the suffix. The test elects the recovered voter and verifies that a retry covered by the snapshot preserves a later overwrite.

The current implementation assembles each transfer in memory and limits the image to 16 MiB. Compaction pauses when the next image would exceed the limit. An interrupted transfer restarts from its first chunk.

| Scenario | Fault or ordering | Required result |
| --- | --- | --- |
| Quorum durability | Pause follower sync during a write with the third node disconnected. Resume sync, observe success, kill the leader, and reconnect the two followers. | No success while only one copy is durable. The surviving quorum elects a leader and reads the acknowledged value. |
| Isolated former leader | Partition the leader from both followers. After the majority elects a leader and overwrites a key, send a fresh read and write to the isolated node. | The majority serves the new value. Neither request to the isolated node succeeds. After healing, the isolated node catches up and any conflicting uncommitted suffix is replaced. |
| Retry after a lost reply | Commit and apply a batch, drop its reply, and kill the leader. On the new leader, overwrite one affected key using a different client identity, then retry the original batch. | The retry returns the original response and preserves the later overwrite. A different command with the same sequence is rejected. |
| Read application barrier | Confirm a read quorum while application of an acknowledged commit on a newly elected leader is paused. | The read waits for application to reach its barrier, then observes the committed value. |
| Restart during application | Stop a node after it learns an entry is committed but before applying it, and again during state-machine persistence. | The node recovers the complete batch and its retry result together. A successful read never exposes a partial batch. |
| Snapshot catch-up | Take one follower offline until its required log prefix is compacted. Interrupt snapshot installation, restart the follower, and resume transfer. | The follower recovers a complete state, installs the snapshot, and applies the suffix. After promotion through an election, it returns current values and honors retries covered by the snapshot. |
| Peer authentication | Present a certificate for an unconfigured peer or a configured certificate with a mismatched claimed identity. | The connection is rejected and cannot contribute a vote, append acknowledgement, or read confirmation. |
