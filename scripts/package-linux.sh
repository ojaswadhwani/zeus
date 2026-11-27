#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

channel=${1:?expected preview or beta}
case "$channel" in
    preview | beta) ;;
    *) echo "Expected preview or beta, got $channel." >&2; exit 1 ;;
esac

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
    echo "The archive must be built on Linux x86-64." >&2
    exit 1
fi

package_id=$(cargo pkgid --locked)
version=${package_id##*[@#]}
if [[ $channel == preview && $version != *-preview ]] ||
   [[ $channel == beta && ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+-beta\.[0-9]+$ ]]; then
    echo "Expected a $channel package version, got $version." >&2
    exit 1
fi

destination=${2:-target/$channel}
build_dir=${CARGO_TARGET_DIR:-target}
cargo build --locked --release --features replication --bins --target-dir "$build_dir"

for binary in zeus-local zeus-node; do
    test "$("$build_dir/release/$binary" --version)" = "$binary $version"
done

bundle="zeus-$version-linux-x86_64"
archive="$bundle.tar.gz"
staging=$(mktemp -d)
trap 'rm -rf -- "$staging"' EXIT
mkdir -p -- "$staging/$bundle" "$destination"
cp -- "$build_dir/release/zeus-local" "$build_dir/release/zeus-node" \
    LICENSE README.md CHANGELOG.md SECURITY.md "$staging/$bundle/"
cp -R -- docs "$staging/$bundle/"
if [[ $channel == beta ]]; then
    cp -- Cargo.lock "$staging/$bundle/"
    cargo tree --locked --features replication --target x86_64-unknown-linux-gnu \
        --edges normal,build --prefix none --format '{p} | {l}' |
        LC_ALL=C sort -u > "$staging/$bundle/RUST-DEPENDENCIES.txt"
fi

tar --sort=name --mtime=@0 --numeric-owner --owner=0 --group=0 \
    -C "$staging" -cf - "$bundle" | gzip -n > "$staging/$archive"
(
    cd -- "$staging"
    sha256sum "$archive" > SHA256SUMS
    sha256sum -c SHA256SUMS
)
mv -- "$staging/$archive" "$staging/SHA256SUMS" "$destination/"
