#!/bin/bash
# Mounts LazyFS with the specified configuration.
# Adapted from upstream mount-lazyfs.sh to use system-installed binary.

set -euo pipefail

HELP_MSG="Mount LazyFS with:
  -c | --config-path  : Path to LazyFS toml config file.
  -m | --mount.dir    : FUSE mount directory.
  -r | --root.dir     : FUSE root directory.
  -s | --single-thread: Run FUSE in single-thread mode.
"

if [ $# -eq 0 ]; then
    echo -e "$HELP_MSG"
    exit 1
fi

SINGLE_THREAD=""
CMD_CONFIG=""
MOUNT_DIR=""
ROOT_DIR=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config-path)
            CMD_CONFIG="$2"
            shift 2
            ;;
        -m|--mount.dir)
            MOUNT_DIR="$2"
            shift 2
            ;;
        -r|--root.dir)
            ROOT_DIR="$2"
            shift 2
            ;;
        -s|--single-thread)
            SINGLE_THREAD="-s"
            shift
            ;;
        -h|--help)
            echo -e "$HELP_MSG"
            exit 0
            ;;
        *)
            echo "Error: Unknown option $1" >&2
            exit 1
            ;;
    esac
done

[ -f "$CMD_CONFIG" ] || { echo "Error: Config file '$CMD_CONFIG' does not exist." >&2; exit 1; }
[ -d "$MOUNT_DIR" ] || { echo "Error: Mount directory '$MOUNT_DIR' does not exist." >&2; exit 1; }
[ -d "$ROOT_DIR" ] || { echo "Error: Root directory '$ROOT_DIR' does not exist." >&2; exit 1; }

/usr/local/bin/lazyfs "$MOUNT_DIR" \
    --config-path "$CMD_CONFIG" \
    -o allow_other \
    -o modules=subdir \
    -o subdir="$ROOT_DIR" \
    $SINGLE_THREAD &
