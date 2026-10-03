#!/usr/bin/env bash
# Real-process smoke test: three raftkvd processes on localhost, driven by raftkvctl.
#
#   tests/cluster/smoke.sh path/to/raftkvd path/to/raftkvctl
#
# 1. The cluster serves put/append/get.
# 2. kill -9 the leader: a new leader serves, and nothing acknowledged is lost.
# 3. kill -9 EVERY node in the middle of a stream of appends, restart all from disk: each
#    acknowledged append is there exactly once, in order.
# 4. A node whose log file is corrupted refuses to start.
#
# Unlike the simulator this is not replayable from a seed; it shows that the real transport,
# real files and real fsync behave like their simulated counterparts.
set -euo pipefail

RAFTKVD=$1
RAFTKVCTL=$2
WORK=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-smoke-XXXXXX")
# Below every OS's ephemeral range (Linux 32768+, macOS 49152+), so an outgoing connection
# can never be holding a port a server is about to listen on.
BASE=$(( 20000 + (RANDOM % 12000) ))
CLUSTER=$WORK/cluster.json
printf '{"1":"127.0.0.1:%d","2":"127.0.0.1:%d","3":"127.0.0.1:%d"}\n' $BASE $((BASE+1)) $((BASE+2)) > "$CLUSTER"
declare -a PID=(0 0 0 0)

cleanup() {
    for i in 1 2 3; do [[ ${PID[$i]} -ne 0 ]] && kill -9 "${PID[$i]}" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$WORK"
}
fail() {
    echo "FAIL: $*" >&2
    for i in 1 2 3; do echo "--- node $i log (tail) ---" >&2; tail -n 30 "$WORK/node$i.log" >&2 || true; done
    # In GitHub Actions, also raise an annotation: unlike job logs, annotations can be read
    # through the public API, so a CI-only failure still leaves evidence.
    if [[ -n ${GITHUB_ACTIONS:-} ]]; then
        local detail="$*"
        # Workflow commands need %, newline and CR escaped; a missing log must not end the script.
        for i in 1 2 3; do
            detail+="%0A--- node $i ---%0A$( { tail -n 8 "$WORK/node$i.log" 2>/dev/null || true; } |
                awk '{ gsub(/%/, "%25"); gsub(/\r/, "%0D"); printf "%s%%0A", $0 }')"
        done
        echo "::error title=cluster_smoke failed::$detail"
    fi
    exit 1
}
trap cleanup EXIT

start() {  # start node $1
    "$RAFTKVD" --id "$1" --cluster "$CLUSTER" --data-dir "$WORK/data$1" >>"$WORK/node$1.log" 2>&1 &
    PID[$1]=$!
}
kill9() {  # kill -9 node $1
    kill -9 "${PID[$1]}" 2>/dev/null || true
    wait "${PID[$1]}" 2>/dev/null || true
    PID[$1]=0
}
ctl() { "$RAFTKVCTL" --cluster "$CLUSTER" --timeout-ms 10000 "$@"; }
# Waits up to $2 seconds for process $1 to exit and sets RC to its exit status; returns 1 if
# it is still running. Nothing in this script waits without a deadline, so a hang fails fast,
# with logs, instead of running into ctest's timeout.
wait_exit() {
    local pid=$1 deadline=$((SECONDS + $2))
    while kill -0 "$pid" 2>/dev/null; do
        (( SECONDS < deadline )) || return 1
        sleep 0.1
    done
    set +e; wait "$pid"; RC=$?; set -e
}
leader_of() {  # run an op, print the id of the server that answered
    ctl --verbose "$@" 2>&1 >/dev/null | sed -n 's/^served-by \([0-9]*\).*/\1/p'
}

echo "== 1. basic operations (ports $BASE-$((BASE+2)))"
for i in 1 2 3; do start $i; done
[[ $(ctl put k v1) == OK ]] || fail "put"
[[ $(ctl append k +a) == OK ]] || fail "append"
[[ $(ctl get k) == "v1+a" ]] || fail "get returned '$(ctl get k)'"
set +e; ctl get missing 2>/dev/null; rc=$?; set -e
[[ $rc -eq 1 ]] || fail "get of a missing key exited $rc, want 1"

echo "== 2. kill -9 the leader"
LEADER=$(leader_of get k)
[[ -n $LEADER ]] || fail "could not find the leader"
kill9 "$LEADER"
[[ $(ctl put k2 v2) == OK ]] || fail "put after leader kill"
[[ $(ctl get k) == "v1+a" ]] || fail "value lost after leader kill: '$(ctl get k)'"
NEW=$(leader_of get k2)
[[ $NEW != "$LEADER" ]] || fail "the killed node $LEADER still answers"
echo "   leader $LEADER killed, node $NEW took over"
start "$LEADER"

echo "== 3. kill -9 every node mid-workload, restart from disk"
ACKED=$WORK/acked
: >"$ACKED"
(
    for n in $(seq 1 1000); do
        if ctl --timeout-ms 1500 append log "$n," >/dev/null 2>&1; then echo "$n," >>"$ACKED"; fi
    done
) &
WRITER=$!
sleep 2
for i in 1 2 3; do kill9 $i; done
kill "$WRITER" 2>/dev/null || true; wait "$WRITER" 2>/dev/null || true
for i in 1 2 3; do start $i; done
GOT=$(ctl get log) || fail "get after full restart"
N_ACKED=$(wc -l <"$ACKED" | tr -d ' ')
[[ $N_ACKED -gt 0 ]] || fail "no append was acknowledged before the kill"
POS=0
while read -r piece; do
    # Each acknowledged piece appears exactly once, and after the previous acknowledged one.
    COUNT=$(tr ',' '\n' <<<"$GOT" | grep -cx "${piece%,}" || true)
    [[ $COUNT -eq 1 ]] || fail "acknowledged append '$piece' appears $COUNT times"
    AT=$(awk -v s="$GOT" -v p=",$piece" 'BEGIN { print index("," s, p) }')
    [[ $AT -gt $POS ]] || fail "acknowledged append '$piece' is out of order"
    POS=$AT
done <"$ACKED"
echo "   $N_ACKED acknowledged appends all survived, once each, in order"

echo "== 4. a corrupted log makes the node refuse to start"
kill9 3
LOG=$WORK/data3/log
SIZE=$(wc -c <"$LOG" | tr -d ' ')
AT=$((SIZE / 2))
BEFORE=$(od -An -tx1 -j "$AT" -N1 "$LOG" | tr -d ' ')
NEW=$([[ $BEFORE == a5 ]] && echo 5a || echo a5)
printf "\\x$NEW" | dd of="$LOG" bs=1 seek="$AT" conv=notrunc 2>/dev/null
AFTER=$(od -An -tx1 -j "$AT" -N1 "$LOG" | tr -d ' ')
# Make sure the corruption really happened, so a pass means "refused", not "nothing to refuse".
[[ $AFTER == "$NEW" ]] || fail "could not corrupt byte $AT of a $SIZE-byte log (was $BEFORE, now $AFTER)"
"$RAFTKVD" --id 3 --cluster "$CLUSTER" --data-dir "$WORK/data3" >>"$WORK/node3.log" 2>&1 &
P3=$!
if ! wait_exit $P3 10; then kill -9 $P3; fail "node 3 is running on a corrupt log (log size $SIZE)"; fi
[[ $RC -eq 1 ]] || fail "node 3 started on a corrupt log (exit $RC)"
grep -q "refusing to start" "$WORK/node3.log" || fail "no refusal message"
[[ $(ctl put after-corruption yes) == OK ]] || fail "the other two stopped serving"

echo "== shut down"
for i in 1 2; do kill -TERM "${PID[$i]}"; done
for i in 1 2; do
    wait_exit "${PID[$i]}" 10 || fail "node $i did not exit within 10 s of SIGTERM"
    PID[$i]=0
    [[ $RC -eq 0 ]] || fail "node $i exited $RC on SIGTERM"
done
echo "PASS"
