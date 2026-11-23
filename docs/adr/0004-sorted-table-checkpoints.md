# Immutable sorted table checkpoints

**Status** — Initial implementation.

`Store::checkpoint` exports the current live key-value state to a new file. The writer creates and syncs a temporary file in the destination directory, publishes it with a hard link that fails if the destination exists, then removes the temporary name and syncs the directory. An error after publication can leave a complete checkpoint at the destination. An interrupted export can leave a temporary file.

The 20-byte header contains the eight bytes `ZEUSTBL1`, a little-endian unsigned 64-bit record count, and a little-endian IEEE CRC-32 of the first 16 bytes. The header is followed by exactly that many `ZWL1` put records in strictly increasing bytewise key order. The reader rejects invalid checksums, truncated records, duplicates, and trailing data. A zero-record table is valid.

`SortedTable::open` verifies the entire file and indexes the keys and value locations in memory. Point reads and range scans fetch values from disk as needed. Operators must keep the checkpoint file immutable after publication. The checkpoint is an independent read-only export. The live store uses the same sorted structure after compaction, with magic `ZEUSDB01` and a WAL tail after the last sorted record. See the [live storage design](0005-live-sorted-base.md).
