#!/bin/bash
# Entrypoint script for LazyFS Docker container.
#
# Mounts LazyFS and then executes the provided command.

set -e

# Create mount directories if they don't exist.
mkdir -p /tmp/lazyfs.root /tmp/lazyfs.mnt

# Create FIFOs for LazyFS communication.
rm -f /tmp/lazyfs.fifo /tmp/lazyfs.done
mkfifo /tmp/lazyfs.fifo
mkfifo /tmp/lazyfs.done

echo "Starting LazyFS mount..."

# Start LazyFS in background.
# Mount point: /tmp/lazyfs.mnt (what applications see)
# Root directory: /tmp/lazyfs.root (actual storage via subdir module)
/opt/lazyfs/lazyfs/build/lazyfs \
    /tmp/lazyfs.mnt \
    --config-path /etc/lazyfs.toml \
    -o allow_other \
    -o modules=subdir \
    -o subdir=/tmp/lazyfs.root \
    -s &

LAZYFS_PID=$!

# Wait for mount to be ready.
sleep 2

# Verify mount succeeded.
if ! mountpoint -q /tmp/lazyfs.mnt; then
    echo "ERROR: LazyFS mount failed"
    exit 1
fi

echo "LazyFS mounted at /tmp/lazyfs.mnt (PID: $LAZYFS_PID)"

# Cleanup function.
cleanup() {
    echo "Cleaning up LazyFS..."
    fusermount -uz /tmp/lazyfs.mnt 2>/dev/null || true
    kill $LAZYFS_PID 2>/dev/null || true
    rm -f /tmp/lazyfs.fifo /tmp/lazyfs.done
}

trap cleanup EXIT

# Execute the provided command.
exec "$@"
