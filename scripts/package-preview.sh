#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
    echo "The preview archive must be built on Linux x86-64." >&2
    exit 1
fi

package_id=$(cargo pkgid --locked)
version=${package_id##*[@#]}
if [[ $version != *-preview ]]; then
    echo "Expected a preview package version, got $version." >&2
    exit 1
fi

destination=${1:-target/preview}
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

tar --sort=name --mtime=@0 --numeric-owner --owner=0 --group=0 \
    -C "$staging" -cf - "$bundle" | gzip -n > "$staging/$archive"
(
    cd -- "$staging"
    sha256sum "$archive" > SHA256SUMS
    sha256sum -c SHA256SUMS
)
mv -- "$staging/$archive" "$staging/SHA256SUMS" "$destination/"
