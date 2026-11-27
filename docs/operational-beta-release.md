# Package and verify Zeus 0.3.0-beta.2

The release targets Linux x86-64. The archive includes both binaries, the lockfile, and `RUST-DEPENDENCIES.txt`, which lists the normal and build dependencies selected for Linux x86-64 with the replication feature. Dynamic system libraries are supplied by the target host.

## Build a local candidate

From a clean candidate checkout with the pinned Rust toolchain:

```sh
cargo test --locked --release --all-features --all-targets
bash scripts/package-linux.sh beta
bash scripts/verify-linux-beta.sh target/beta 0.3.0-beta.2 target/verified-beta
```

Run the [upgrade rehearsal](operational-beta-upgrade.md#rehearse-the-upgrade) with `ZEUS_CANDIDATE_BIN` set to the absolute path to the extracted `zeus-node`.

## Hosted release

A manual **Operational beta package** run builds and tests a candidate for review. The run for the approved `v0.3.0-beta.2` tag signs the archive through GitHub artifact attestations. It uploads the archive and checksums to a draft prerelease, downloads them back, verifies the signature against the repository, tag, workflow and commit, and checks the extracted package before publishing.

GitHub artifact attestations must be available for the repository. If hosted verification fails, the release stays in draft. Inspect the failed step and the draft assets before attempting publication again.

## Verify a published download

Download the archive and `SHA256SUMS` into a fresh directory. With an authenticated GitHub CLI that supports artifact attestations, verify the archive before extracting it.

```sh
repo=ojaswadhwani/zeus
version=0.3.0-beta.2
bundle="zeus-$version-linux-x86_64"
gh release download "v$version" --repo "$repo" --pattern "$bundle.tar.gz" --pattern SHA256SUMS
sha256sum --strict -c SHA256SUMS
gh attestation verify "$bundle.tar.gz" --repo "$repo" \
  --signer-workflow "$repo/.github/workflows/operational-beta.yml" \
  --source-ref "refs/tags/v$version"
tar -xzf "$bundle.tar.gz"
"./$bundle/zeus-node" --version
"./$bundle/zeus-local" --version
```

Both binaries should report the requested beta version. The inventory is included in the attested archive. Run the [three-voter quickstart](replication-quickstart.md) or follow the [upgrade procedure](operational-beta-upgrade.md) on the intended host.
