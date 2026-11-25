# Changelog

## 0.1.0 alpha (2026-11-25)

- Embedded Rust library with durable puts and deletes, atomic write batches, and ordered scans.
- Disk-backed values, an in-memory key index, and manual compaction into a sorted base with a write-ahead log tail.
- Read-only sorted checkpoint export and an example CLI for local operation.
- Existing `ZWL1`, `ZWD1`, and `ZWB1` logs remain readable. A compacted database starts with `ZEUSDB01` and requires a build that supports that format.
- Point reads and range scans return I/O errors and owned values because values are fetched from disk.
- Linux x86-64 is the initial release target. Embedded compaction is synchronous and needs temporary space for a copy of live values.
