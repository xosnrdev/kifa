#!/bin/bash
# Mounts LazyFS and runs crash-test.

set -euo pipefail

LAZYFS_CONFIG="/etc/lazyfs/lazyfs.toml"
LAZYFS_MOUNT="/mnt/lazyfs"
LAZYFS_ROOT="/data/lazyfs"

cleanup() {
    if mountpoint -q "${LAZYFS_MOUNT}" 2>/dev/null; then
        fusermount -uz "${LAZYFS_MOUNT}" 2>/dev/null || true
    fi
}

trap cleanup EXIT

[ -e /dev/fuse ] || { echo "Error: /dev/fuse not available" >&2; exit 1; }

mkdir -p "${LAZYFS_MOUNT}" "${LAZYFS_ROOT}"
rm -rf "${LAZYFS_ROOT:?}"/* 2>/dev/null || true

mount-lazyfs.sh -c "${LAZYFS_CONFIG}" -m "${LAZYFS_MOUNT}" -r "${LAZYFS_ROOT}" -s

retries=20
while ! mountpoint -q "${LAZYFS_MOUNT}" && [ $retries -gt 0 ]; do
    sleep 0.25
    retries=$((retries - 1))
done
[ $retries -gt 0 ] || { echo "Error: Failed to mount LazyFS" >&2; exit 1; }

retries=20
while [ ! -p /tmp/lazyfs.fifo ] && [ $retries -gt 0 ]; do
    sleep 0.25
    retries=$((retries - 1))
done
[ $retries -gt 0 ] || { echo "Error: LazyFS FIFO not created" >&2; exit 1; }

crash-test \
    --data-dir "${LAZYFS_MOUNT}" \
    --lazyfs \
    --kifa-bin /usr/local/bin/kifa \
    --gen-transactions-bin /usr/local/bin/gen-transactions \
    --skip-build \
    "$@"
