# raftkv

A replicated key–value store in C++20, built on the Raft consensus algorithm.
It runs as 3 or 5 processes on one machine, talking over real sockets, and keeps working when a minority of them crash.
It is a learning project: one Raft group, no transactions or indexes, and it isn't built to be fast.

> **Status:** Phase 8 — a working replicated KV store with snapshots and log compaction, a passing fault matrix (below), and 540 fault-injected histories checked linearizable by Porcupine. Benchmarks (Phase 9) are next.

## What it will do

- **Replicate:** every node applies the same writes in the same order.
- **Survive failures:** if the leader dies, the others elect a new one, and no acknowledged write is lost.
- **Fail safely:** a node cut off from the majority refuses writes instead of returning wrong answers.
- **Recover from crashes:** state is saved to disk with checksums, so a node can be killed with `kill -9` and restart cleanly.
- **Serve a simple KV API:** `Get`, `Put` and `Append`, with duplicate detection so a retried request is applied only once.
- **Compact its log:** snapshots keep the on-disk log bounded. Over 100,000 operations it never held more than about 200 entries (11 KB), and a node that was down from the start catches up from a snapshot in under 60 ms.

## How it will be tested

- **Deterministic simulator:** the whole cluster runs in one process on a virtual clock with a seeded network, so any failure replays exactly from `--seed N`.
- **Fault matrix:** crashes, partitions, message drops, delays and corrupted logs are all injected by automated tests.
- **Linearizability checking:** recorded operation histories are checked with [Porcupine](https://github.com/anishathalye/porcupine), through a small Go tool in `tools/lincheck` (a dev dependency only). See [`docs/lincheck/`](docs/lincheck/) for what a failure looks like.
- **Memory and thread safety:** CI runs ASan, UBSan and TSan, and the log decoder is fuzzed with libFuzzer.
- **Benchmarks:** throughput, latency and failover time, written to `docs/results.csv`.

## Fault matrix

Every row is an automated test. In the simulator, each row runs under 20 seeds (any one
replays with `RAFTKV_SEED=N`), with 3 servers and 3 clients doing get/put/append. The Raft
invariants are checked after every event, and every run's history goes through the
linearizability checker, which is what "no acknowledged write lost" means here. Rows marked
in the last column also run against real `raftkvd` processes (`tests/cluster/smoke.sh`).

| Scenario | Injection | Expected | Measured | Simulator test | Real processes |
|---|---|---|---|---|---|
| Leader crash mid-write | kill the leader after it sent `AppendEntries`, before commit | new leader ≈1 s; no acknowledged write lost | new leader in 241 ms median, 496 ms max | `LeaderCrashMidWrite` | ✓ every node `kill -9`'d mid-write |
| Minority partition | cut 1 of 3 off (leader or follower) | majority serves; minority refuses writes | the cut-off node commits nothing | `MinorityPartition` | |
| Majority partition | cut every server off from every other | **unavailable, not inconsistent** | nothing commits, no op completes; consistent after healing | `MajorityPartition` | |
| Follower crash + restart | `kill -9` a follower, restart 1 s later | catches up | recovers from its own log, then catches up | `FollowerCrashAndRestart` | ✓ all nodes `kill -9` |
| Slow follower: slow replies | +500 ms on its replies to the leader | available; p50 unchanged | available; p50 **+12%**, throughput −10% (median of 20 seeds); no extra elections. Commits wait for the one fast follower instead of the faster of two | `SlowFollowerReplies` | |
| Slow follower: slow link both ways | +500 ms each way | available; p50 unchanged | available; p50 +2%, throughput −7% median, **−22% worst**. The slow follower keeps timing out and calling elections (98 over 20 seeds): no PreVote | `SlowFollowerLinkBothWays` | |
| Message drops | 20% of all messages, 5 s | progress continues via retries | every client progresses, at **5.2%** of normal throughput: each loss costs the client a 500 ms timeout | `TwentyPercentMessageDrops` | |
| Paused leader | `SIGSTOP` the leader for 1 s | followers elect; old leader steps down on resume | as expected | `PausedLeader` | |
| Torn log tail | half a record at the end of a follower's log | cuts it off, rejoins, catches up | as expected | `TornLogTail` | |
| Corrupt log tail | flip a byte in a complete, synced record | refuses to start; no stale reads | refuses to start; the other two keep serving | `CorruptLogTail` | ✓ |

Three rows did not match the plan's expectation, and the table says so rather than tuning
the tests until they did. The slow-replies latency cost is inherent to a 3-node cluster
with one slow member. The slow-link elections come from having no PreVote. The 20%-drop
slowdown comes from the client, which waits a full 500 ms and then moves to another server
after any loss; Phase 9 measures that before anything is changed.

## Stack

C++20 · CMake + Ninja · vcpkg · standalone Asio · Protocol Buffers · GoogleTest · Google Benchmark · GitHub Actions

## Building

Needs CMake ≥ 3.25, Ninja, a C++20 compiler and [vcpkg](https://github.com/microsoft/vcpkg) with `VCPKG_ROOT` set.

```sh
cmake --preset dev          # Debug + ASan/UBSan; also: tsan, rel
cmake --build --preset dev
ctest --preset dev
```

The first configure builds all dependencies through vcpkg, which takes a few minutes.

## Running a cluster

```sh
cat > cluster.json <<'JSON'
{ "1": "127.0.0.1:7001", "2": "127.0.0.1:7002", "3": "127.0.0.1:7003" }
JSON
for i in 1 2 3; do ./build/rel/raftkvd --id $i --cluster cluster.json --data-dir data/$i & done

./build/rel/raftkvctl --cluster cluster.json put greeting hello
./build/rel/raftkvctl --cluster cluster.json append greeting ", world"
./build/rel/raftkvctl --cluster cluster.json get greeting     # hello, world
```

Kill any one server with `kill -9` and the other two keep serving; restart it and it
catches up from its data directory.

## Out of scope for v1

Membership changes, `ReadIndex`/lease reads, a gRPC front end, and sharding across multiple Raft groups.

## More detail

- [`raftkv.md`](raftkv.md): goals, scope and what counts as done
- [`raftkv-implementation.md`](raftkv-implementation.md): the phase-by-phase build plan
- [`docs/postmortems/`](docs/postmortems/): every real bug found so far, what caused it and how it was caught
