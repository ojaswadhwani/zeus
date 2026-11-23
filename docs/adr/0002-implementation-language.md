# Implementation language

**Status** — Accepted for the prototype and planned releases.

Use Rust for the embedded engine and server. The standard library provides the file I/O and locking needed for the local prototype, and safe Rust checks ownership and shared access in the storage code. Keep the prototype dependency-free. Evaluate Raft and TLS dependencies when their integration requirements are concrete.

Port the prototype with the same ZWL1 byte layout and write acknowledgement contract. Keep recovery tests independent of the implementation language wherever practical. [Write acknowledgement and recovery](0001-write-durability.md)
