# Log compaction

**Status** — Implemented for the 0.1 alpha candidate.

`Store::compact()` writes a sorted base table with one `ZWL1` put record per live key in bytewise key order. Subsequent writes append WAL records to the same file. It syncs the replacement file, atomically renames it over the current log, then syncs the containing directory before reporting success. The replacement encodes the same logical state as the original, including the result of any committed batches and deletes. An interruption before publication leaves the original log authoritative. Temporary files left by an interrupted compaction are ignored on open and can be removed when all writers are stopped.

The log must have one directory entry and reside in a stable operator-controlled directory. The compactor holds locks on both inodes at publication. An opener checks that the inode it locked is the one currently at the log path before replay. Use compaction-aware builds for all processes accessing a log during compaction. If directory sync fails after publication, drop and reopen the store before writing again.

Compaction is synchronous and needs temporary disk space for one full copy of the live data. Normal writes can continue through the same handle after compaction succeeds. Keys and value locations stay in memory. Values are read from disk. Compaction builds a replacement key index before publication, so enough memory is required for both indexes during compaction.
