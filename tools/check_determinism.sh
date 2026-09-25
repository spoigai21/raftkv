#!/usr/bin/env bash
# Determinism lint (raftkv-implementation.md §3.2). Code that runs inside the simulator must
# get time, timers and randomness from the injected Env, never from the OS or the stdlib.
set -euo pipefail
cd "$(dirname "$0")/.."

dirs=()
for d in src/raft src/kv src/sim; do [[ -d $d ]] && dirs+=("$d"); done
[[ ${#dirs[@]} -eq 0 ]] && { echo "determinism lint: nothing to check"; exit 0; }

pattern='steady_clock|system_clock|high_resolution_clock|sleep_for|sleep_until|std::thread|std::jthread|random_device|\brand\(|\bsrand\(|_distribution\b|\btime\(|unordered_(map|set)'

# A line may opt out with a trailing "// determinism-ok: <reason>", e.g. an unordered_map
# that is only ever looked up, never iterated.
if grep -rnE "$pattern" --include='*.hpp' --include='*.cpp' --include='*.h' "${dirs[@]}" \
        | grep -v 'determinism-ok'; then
    echo "determinism lint: forbidden construct in simulated code (see raftkv-implementation.md §3.2)" >&2
    exit 1
fi
echo "determinism lint: ok (${dirs[*]})"
