# Zeus security and AI direction

Build security into the file formats, protocol, and release process from the first milestone. Design the storage API so AI applications can use it through ordinary clients, and evaluate AI-specific features against real workloads.

## Security foundations

- **Define trust boundaries early.** The initial cluster assumes trusted hosts and administrators and handles crash failures. Treat network requests, import files, and backups as untrusted inputs. Document separately what happens if a node, host, or credential is compromised.
- **Bound and validate inputs before allocating.** Apply explicit limits to frames, keys, values, batches, scan pages, queues, and concurrent connections. Version disk and wire formats. Use checksums for accidental corruption, validate framing and lengths during recovery, and fuzz parsers and recovery code with bounded runtimes.
- **Authenticate before shipping the network preview.** Use an established TLS implementation for client transport and mutual TLS for peer transport. Bind peer identity to cluster membership. Give client read, write, and administrative operations explicit permissions with deny-by-default checks. Keep network endpoints loopback-only until credentials are configured. Test certificate expiry and unauthorized access. [TLS 1.3 specification](https://www.rfc-editor.org/info/rfc9846)
- **Protect operator data.** Keep values, credentials, and key contents out of routine logs and telemetry. Record administrative events with principal, action, and outcome. Document encrypted volumes and encrypted backup storage for initial deployments. Review native at-rest encryption and key rotation when a deployment requires a separate storage-layer key boundary.
- **Make releases verifiable.** Minimize dependencies, pin toolchains and CI dependencies, use least-privilege CI credentials, and publish artifact signatures and build provenance by beta. Before the first public tag, publish a private vulnerability reporting route in `SECURITY.md` and name who will triage reports. [SLSA build and provenance specification](https://slsa.dev/spec/v1.2/)

## AI integration sequence

1. **Start with application state.** Target job state, checkpoint metadata, and retrieval metadata under the normal key-value API. Use these as benchmark workloads, including frequent small updates, ordered scans, and retry behavior after failover. Keep model calls, embeddings, and agent decisions outside the replicated state machine.
2. **Seed an integration boundary before adding agent tooling.** Give each integration a distinct principal and bounded query limits. If there is demand for an MCP adapter, ship it as a separate opt-in process. Start with read-only tools scoped to explicitly allowed data and bounded result sizes. Treat retrieved values as untrusted data. Require distinct credentials and explicit authorization for any later write or administrative tools. [MCP security best practices](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices)
3. **Evaluate vector search after a real workload surfaces.** Use an external vector index first. Evaluate a native index against measured recall, p99 query latency, memory cost, and rebuild time, and document consistency between vectors and source records before committing to it.

## AI assisted development

Require human review for changes to persistence, replication, cryptography, authorization, and release workflows, regardless of how the change was authored. Verify behavior with tests and review the provenance and licensing of incorporated code. Use synthetic fixtures when working with external AI tools and keep production data and secrets out of prompts.
