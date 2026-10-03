#!/usr/bin/env bash
# The raftkv demo (implementation guide Phase 10), as one command:
#
#   tools/demo.sh            # interactive: paced, with a countdown before the kill
#   tools/demo.sh --check    # the same run without pauses; fails unless it all holds (ctest)
#
# 1. Starts a 3-node cluster of real raftkvd processes.
# 2. Four clients write (and read) continuously; the throughput prints every second.
# 3. kill -9 of the leader, mid-stream. Writes stall, a new leader is elected, writes resume.
# 4. Every operation the clients made is checked for linearizability by Porcupine.
#
# Needs the release build (BUILD=build/rel by default) and Go, for tools/lincheck.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD=${BUILD:-$ROOT/build/rel}
CHECK=0
[[ ${1:-} == --check ]] && CHECK=1
SECONDS_OF_LOAD=20   # interactive: long enough to watch the dip and the recovery
KILL_AFTER=8
[[ $CHECK == 1 ]] && { SECONDS_OF_LOAD=12; KILL_AFTER=5; }

if ! command -v go >/dev/null 2>&1; then
    [[ ${RAFTKV_REQUIRE_GO:-0} == 1 ]] && { echo "demo: Go is required" >&2; exit 1; }
    echo "demo: Go is not installed (needed for the linearizability check); skipping"
    exit 77
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-demo-XXXXXX")
BASE=$(( 20000 + (RANDOM % 12000) ))
CLUSTER=$WORK/cluster.json
printf '{"1":"127.0.0.1:%d","2":"127.0.0.1:%d","3":"127.0.0.1:%d"}\n' $BASE $((BASE+1)) $((BASE+2)) > "$CLUSTER"
declare -a PID=(0 0 0 0)
cleanup() {
    for i in 1 2 3; do [[ ${PID[$i]} -ne 0 ]] && kill -9 "${PID[$i]}" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

say() { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }
beat() { [[ $CHECK == 1 ]] || sleep "$1"; }
fail() { echo "demo: FAIL: $*" >&2; exit 1; }

(cd "$ROOT/tools/lincheck" && go build -o "$WORK/lincheck" .)

say "Starting a 3-node raftkv cluster (real processes, durable fsync)"
for i in 1 2 3; do
    "$BUILD/raftkvd" --id $i --cluster "$CLUSTER" --data-dir "$WORK/data$i" >"$WORK/node$i.log" 2>&1 &
    PID[$i]=$!
done
for _ in $(seq 1 50); do
    "$BUILD/raftkvctl" --cluster "$CLUSTER" --timeout-ms 2000 put hello world >/dev/null 2>&1 && break
    sleep 0.2
done
LEADER=$("$BUILD/raftkvctl" --cluster "$CLUSTER" --verbose get hello 2>&1 >/dev/null | sed -n 's/^served-by \([0-9]*\).*/\1/p')
[[ -n $LEADER ]] || fail "the cluster never came up"
echo "  nodes 1, 2, 3 up; node $LEADER is the leader"
beat 2

say "Four clients writing and reading continuously, for ${SECONDS_OF_LOAD}s"
"$BUILD/raftkvload" --cluster "$CLUSTER" --clients 4 --seconds $SECONDS_OF_LOAD --warmup-ms 0 --keys 3 \
    --history "$WORK/history.json" --progress >"$WORK/summary.json" &
LOAD=$!
if [[ $CHECK == 1 ]]; then
    sleep $KILL_AFTER
else
    sleep $((KILL_AFTER - 3))
    for n in 3 2 1; do printf '\033[1m  killing the leader in %s…\033[0m\n' $n >&2; sleep 1; done
fi

say "kill -9 the leader (node $LEADER)"
kill -9 "${PID[$LEADER]}"
wait "${PID[$LEADER]}" 2>/dev/null || true
PID[$LEADER]=0
wait $LOAD || fail "the load generator failed"
NEW=$("$BUILD/raftkvctl" --cluster "$CLUSTER" --verbose get hello 2>&1 >/dev/null | sed -n 's/^served-by \([0-9]*\).*/\1/p')
GAP=$(sed -n 's/.*"max_gap_ms": \([0-9.]*\).*/\1/p' "$WORK/summary.json")
OPS=$(sed -n 's/.*"ops": \([0-9]*\).*/\1/p' "$WORK/summary.json")
echo "  node $NEW took over; the longest pause in completed operations was ${GAP} ms; ${OPS} operations in all"
[[ -n $NEW && $NEW != "$LEADER" ]] || fail "no new leader"
awk -v g="$GAP" 'BEGIN { exit !(g < 5000) }' || fail "writes did not resume within 5 s"
beat 2

say "Checking every operation for linearizability (Porcupine)"
"$WORK/lincheck" -viz "$WORK" "$WORK/history.json" || fail "the history is NOT linearizable (see $WORK)"
beat 1

say "One limit, stated plainly: every write waits for a 4 ms F_FULLFSYNC, once per request"
echo "  (no group commit yet), so a node tops out near 250 writes/s; and the pause above is"
echo "  mostly the client's 500 ms request timeout, not the election."
echo
echo "demo: PASS"
