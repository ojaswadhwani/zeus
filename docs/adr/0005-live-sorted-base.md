# Live sorted base and disk-backed values

**Status** — Implemented for the 0.1 alpha candidate.

Keep the sorted base and append-only log tail in a single file so publishing a compacted base requires one atomic pathname replacement. Legacy WAL-only files remain readable. Compaction writes the sorted format with `ZEUSDB01` in place of the checkpoint magic, followed by the same count, header checksum, and sorted put records. Any following bytes are ordinary WAL records. Older builds reject a compacted database.

Open validates the base strictly and then replays the tail. Incomplete final WAL records can be truncated under the existing recovery rules. A truncated or corrupt sorted base fails opening. The standalone checkpoint reader requires the checkpoint magic and an exact end after the last base record.

The in-memory index retains keys, value offsets, lengths, and checksums. Point reads and ordered scans fetch and validate values on demand. `Store::get` returns `io::Result<Option<Vec<u8>>>` and each scan item returns an I/O result with a borrowed key and owned value. A batch publishes its index entries only after syncing the complete record. Replay indexes values at their locations inside that record.

A single tier keeps the file set and crash protocol small. Reopen still reads the base to verify checksums, and the key index must fit in memory. Compaction rewrites all live values synchronously and temporarily holds a second index. A workload whose values dominate its footprint can exceed RAM if its key index fits.
