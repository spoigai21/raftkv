#!/usr/bin/env bash
# Records linearizability histories with the simulator and checks them with Porcupine.
#
#   tests/lincheck/run.sh path/to/raftkv_tests path/to/source-root path/to/work-dir
#
# Needs Go, for tools/lincheck. Without it this exits 77, which ctest reports as skipped,
# unless RAFTKV_REQUIRE_GO=1 (set in CI), in which case a missing Go is an error.
# Failing histories get an HTML visualization in <work-dir>/viz.
set -euo pipefail

TESTS=$1
SRC=$2
WORK=$3

if ! command -v go >/dev/null 2>&1; then
    if [[ ${RAFTKV_REQUIRE_GO:-0} == 1 ]]; then
        echo "lincheck: Go is required (RAFTKV_REQUIRE_GO=1) but not installed" >&2
        exit 1
    fi
    echo "lincheck: Go is not installed; skipping (install Go to check linearizability)"
    exit 77
fi

rm -rf "$WORK"
mkdir -p "$WORK/histories" "$WORK/viz"
(cd "$SRC/tools/lincheck" && go build -o "$WORK/lincheck" .)

RAFTKV_HISTORY_DIR="$WORK/histories" "$TESTS" --gtest_filter='Linearizability.*:FaultMatrix.*' --gtest_brief=1
N=$(find "$WORK/histories" -name '*.json' | wc -l | tr -d ' ')
[[ $N -gt 0 ]] || { echo "lincheck: no histories were written" >&2; exit 1; }

"$WORK/lincheck" -viz "$WORK/viz" "$WORK"/histories/*.json
