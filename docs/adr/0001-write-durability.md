# Write acknowledgement and recovery

**Status** — Initial implementation.

## Decision

Use a single checksummed append-only write-ahead log and replay it into an in-memory index of keys and value locations. After compaction, replay starts from a [sorted base](0005-live-sorted-base.md). A successful put means the complete record has been written and the file sync succeeded. The store syncs the file and containing directory on open before accepting writes. The caller supplies an existing, durably created directory controlled by the store operator. Use a local Linux filesystem and storage that honor sync requests.

Writes require exclusive access to the store handle. A nonblocking exclusive file lock prevents multiple cooperating handles from opening the same file. Newly created logs use owner-only permissions. If an append or sync fails, reopen before attempting another write. A write without a successful acknowledgement may appear after recovery if its entire record survived.

## Record format

A put starts with the four magic bytes `ZWL1`, followed by the key length, value length, CRC-32 (IEEE) of the concatenated key and value, and CRC-32 (IEEE) of the preceding 16 header bytes. Each numeric field is an unsigned 32-bit little-endian integer. Key bytes and then value bytes follow the 20-byte header. Keys contain 1 to 4096 bytes. Values contain up to 1 MiB and may be empty.

A delete uses `ZWD1` with the same layout, a zero value length, and a checksum over the key bytes. Deleting a missing key returns false without adding a record. A successful deletion of a live key is synced before acknowledgement. Readers that predate delete support reject logs containing its records.

An atomic batch uses `ZWB1` with zero key length. The value field holds the batch body and its checksum covers the entire body. Each operation starts with a one-byte opcode (1 for put, 2 for delete), followed by four-byte little-endian key and value lengths, then the key and value bytes. Deletes carry zero value bytes. The existing key and value size limits apply to each operation. A batch may contain up to 1,024 operations and 4 MiB of encoded body. Operations execute in order, including repeated operations on the same key. An empty batch writes no record.

A complete batch is validated before replay applies any operation. An incomplete final batch is discarded as a whole. A successful batch uses one file sync. Older readers reject logs containing batch records.

Recovery validates complete headers and record bodies in order. An incomplete final header or a validated header with an incomplete final body is truncated to the last complete record. A complete record with a failed integrity check aborts open. These checksums detect accidental corruption. They provide no authentication.

## Consequences

Each write pays for a file sync. Keys and value locations must fit in memory. Reopen validates the sorted base when present and replays the log tail. The prototype provides put, get, delete, ordered scans, and atomic write batches. The in-memory index orders keys lexicographically by bytes and scans use an inclusive start and exclusive end. Manual [log compaction](0003-log-compaction.md) reclaims obsolete records. Replication is a subsequent milestone.
