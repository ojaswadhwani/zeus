#!/usr/bin/env bash
set -euo pipefail

distribution=$(realpath -- "${1:?expected distribution directory}")
version=${2:?expected beta version}
destination=${3:?expected extraction directory}
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+-beta\.[0-9]+$ ]]
bundle="zeus-$version-linux-x86_64"
archive="$bundle.tar.gz"
(
    cd -- "$distribution"
    test "$(wc -l < SHA256SUMS)" -eq 1
    read -r digest filename < SHA256SUMS
    [[ $digest =~ ^[[:xdigit:]]{64}$ && $filename == "$archive" ]]
    sha256sum --strict -c SHA256SUMS
)
mkdir -- "$destination"
tar -xzf "$distribution/$archive" -C "$destination"
cd -- "$destination/$bundle"
test "$(./zeus-node --version)" = "zeus-node $version"
test "$(./zeus-local --version)" = "zeus-local $version"
grep -Eq '^raft v[0-9]' RUST-DEPENDENCIES.txt
test -s Cargo.lock
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
./zeus-local "$scratch/smoke.wal" put release verified
test "$(./zeus-local "$scratch/smoke.wal" get release)" = verified
