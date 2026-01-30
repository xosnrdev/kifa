#!/usr/bin/env bash
# Crash-test harness for Kifa.
#
# Repeatedly starts Kifa with piped transactions, kills it mid-write with
# SIGKILL, then verifies data integrity via the stats command. Proves that
# fsynced entries survive unclean shutdowns.

set -euo pipefail

# --- Defaults ---

CYCLES=100
DATA_DIR="/tmp/kifa-crash-test"
RATE=500
FLUSH_MODE="cautious"
CLEAN=false

# --- Argument parsing ---

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -n, --cycles <N>       Number of crash cycles [default: $CYCLES]
  -d, --data-dir <DIR>   Data directory [default: $DATA_DIR]
  -r, --rate <TPS>       Transaction generation rate [default: $RATE]
  --flush-mode <MODE>    Flush mode to test [default: $FLUSH_MODE]
  --clean                Remove data directory before starting
  -h, --help             Show this help message
EOF
}

require_arg() {
    if [[ -z "${2:-}" ]]; then
        echo "Error: $1 requires a value" >&2
        exit 1
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--cycles)
            require_arg "$1" "${2:-}"
            CYCLES="${2}"
            shift 2
            ;;
        -d|--data-dir)
            require_arg "$1" "${2:-}"
            DATA_DIR="${2}"
            shift 2
            ;;
        -r|--rate)
            require_arg "$1" "${2:-}"
            RATE="${2}"
            shift 2
            ;;
        --flush-mode)
            require_arg "$1" "${2:-}"
            FLUSH_MODE="${2}"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if ! [[ "$CYCLES" =~ ^[0-9]+$ ]] || [[ "$CYCLES" -lt 1 ]]; then
    echo "Error: --cycles must be an integer >= 1" >&2
    exit 1
fi

if ! [[ "$RATE" =~ ^[0-9]+$ ]] || [[ "$RATE" -lt 1 ]]; then
    echo "Error: --rate must be an integer >= 1" >&2
    exit 1
fi

# --- Cleanup trap ---

cleanup() {
    pkill -9 -f "kifa daemon.*${DATA_DIR}" 2>/dev/null || true
    pkill -9 -f "gen-transactions.*-s [0-9]" 2>/dev/null || true
}
trap cleanup EXIT

# --- Build phase ---

echo "Building kifa and gen-transactions..."
cargo build --release --workspace --examples 2>&1

KIFA="./target/release/kifa"
GEN="./target/release/examples/gen-transactions"

if [[ ! -x "$KIFA" ]]; then
    echo "Error: kifa binary not found at $KIFA" >&2
    exit 1
fi

if [[ ! -x "$GEN" ]]; then
    echo "Error: gen-transactions binary not found at $GEN" >&2
    exit 1
fi

# --- Setup phase ---

if [[ "$CLEAN" == true ]]; then
    rm -rf "$DATA_DIR"
fi
mkdir -p "$DATA_DIR"

echo ""
echo "=== Kifa Crash Test ==="
echo "Flush mode: $FLUSH_MODE"
echo "Cycles:     $CYCLES"
echo "Rate:       $RATE txn/s"
echo "Data dir:   $DATA_DIR"
echo ""

passed=0
failed=0
prev_entries=0
gap_cycles=0

# --- Crash cycle loop ---

for (( i = 1; i <= CYCLES; i++ )); do
    # Start the generator piped into Kifa in the background. Seed is the cycle
    # number so each cycle produces a distinct but reproducible stream.
    "$GEN" -n 0 -r "$RATE" -s "$i" | \
        "$KIFA" daemon --stdin -d "$DATA_DIR" --flush-mode "$FLUSH_MODE" &>/dev/null &
    pipeline_pid=$!

    # Wait a random duration (1-5 seconds) so kills land at different points
    # in the write/flush/compaction lifecycle.
    sleep_secs=$(( (RANDOM % 5) + 1 ))
    sleep "$sleep_secs"

    # Simulate a power cut. SIGKILL gives no chance for graceful shutdown.
    kill -9 "$pipeline_pid" 2>/dev/null || true
    wait "$pipeline_pid" 2>/dev/null || true

    # Clean up any orphaned child processes from the pipeline.
    pkill -9 -f "kifa daemon.*${DATA_DIR}" 2>/dev/null || true
    pkill -9 -f "gen-transactions.*-s ${i}" 2>/dev/null || true
    sleep 1

    # Run stats to trigger WAL recovery and capture the output.
    stats_output=$("$KIFA" stats -d "$DATA_DIR" 2>&1) || true

    entries=$(echo "$stats_output" | grep "Total entries:" | awk '{print $NF}')
    gaps=$(echo "$stats_output" | grep "Gaps:" | awk '{print $NF}')

    # Default to safe values when parsing fails.
    entries="${entries:-0}"
    gaps="${gaps:-unknown}"

    # Verify: no gaps and monotonically non-decreasing entry count.
    cycle_pass=true

    if [[ "$gaps" != "none" ]]; then
        cycle_pass=false
        gap_cycles=$((gap_cycles + 1))
    fi

    if [[ "$entries" -lt "$prev_entries" ]]; then
        cycle_pass=false
    fi

    if [[ "$cycle_pass" == true ]]; then
        printf "[%3d/%d] PASS  entries=%-8s gaps=%s\n" "$i" "$CYCLES" "$entries" "$gaps"
        passed=$((passed + 1))
    else
        printf "[%3d/%d] FAIL  entries=%-8s gaps=%s (prev=%s)\n" \
            "$i" "$CYCLES" "$entries" "$gaps" "$prev_entries"
        failed=$((failed + 1))
    fi

    prev_entries="$entries"
done

# --- Summary ---

echo ""
echo "=== Summary ==="
echo "Cycles:    $passed/$CYCLES passed"
echo "Entries:   $prev_entries verified"
echo "Gaps:      $gap_cycles"
echo "Data loss: $failed"

if [[ "$failed" -gt 0 ]]; then
    exit 1
fi
