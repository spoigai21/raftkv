# raftkv

A replicated key–value store in C++20, built on the Raft consensus algorithm.
It runs as 3 or 5 processes on one machine, talking over real sockets, and keeps working when a minority of them crash.
It is a learning project: one Raft group, no transactions or indexes, and it isn't built to be fast.

> **Status:** Phase 9 — a working replicated KV store with snapshots, a passing fault matrix, 540 fault-injected histories checked linearizable by Porcupine, and measured results (below). The write-up and demo (Phase 10) remain.

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

## Results

<!-- results:begin (generated by tools/results_table.py from docs/results.csv; do not edit) -->
Measured on Apple M5, 10 cores, 16 GiB, macOS 26.6.2, on 2026-10-03. Real `raftkvd` processes on one machine, driven by `raftkvload` (closed-loop clients, 16-byte values), durable fsync unless stated. Throughput figures are the median of three 10 s runs (the build-type rows are single runs). They compare configurations on this laptop; they are not a claim about production hardware.

| Cluster size (writes, 32 clients) | Throughput | Write p50 | Write p99 |
|---|---|---|---|
| 1 node | 250 ops/s | 128.10 ms | 147.90 ms |
| 3 nodes | 210 ops/s | 154.60 ms | 200.50 ms |
| 5 nodes | 194 ops/s | 161.20 ms | 239.80 ms |

| Reads vs writes (3 nodes, 8 clients, 50% writes) | p50 | p99 |
|---|---|---|
| Get (through the log) | 54.66 ms | 76.54 ms |
| Put | 54.68 ms | 82.61 ms |

| Measurement | Result |
|---|---|
| Recovery after `kill -9` of the leader (3 nodes, 5 kills) | median 495 ms, max 503 ms |
| Throughput with one follower stopped (`SIGSTOP`; stands in for a minority partition) | 214 ops/s, vs 210 with all three |
| Batching: up to 64 entries per `AppendEntries` vs 1 | 214 vs 63 ops/s (**3.39×**) |
| Cost of durable fsync | 216 ops/s durable vs 119,900 without fsync (**554.3×**) |
| Servers built release | 213 ops/s |
| Servers built asan+ubsan | 197 ops/s |
| Servers built tsan | 185 ops/s |
| 20% message drops (simulator, virtual time) | 5.2% of normal throughput |

| Snapshot cost (state size) | Take (serialize) | Restore | Save durably + compact |
|---|---|---|---|
| 1,000 keys | 0.06 ms | 0.13 ms | 16.38 ms |
| 10,000 keys | 0.66 ms | 1.50 ms | 17.75 ms |
| 100,000 keys | 6.49 ms | 14.40 ms | 24.05 ms |

One log append plus sync takes 4,056 µs with fsync and 6.5 µs without, which is the floor on every committed write's latency.

**Predictions**, committed before measuring ([`raftkv.md`](raftkv.md) §7):

| Prediction | Measured | Verdict |
|---|---|---|
| 3 → 5 nodes costs about 25% of write throughput | 7.3% | ✗ wrong: much smaller |
| Writes resume within 1000 ms of `kill -9` of the leader | median 495 ms, max 503 ms | ✓ held |
| Batching gives about 2× | 3.4× | ✗ wrong: underestimated |

**What the numbers say.** Every write is bound by fsync. One append plus `F_FULLFSYNC` takes 4.06 ms, a ceiling of about 247 syncs per second, and a single node reaches 250 ops/s: the leader syncs once per proposal, with no group commit across concurrent requests. Without fsync the same cluster does 119,900 ops/s. That is why extra followers cost so little (followers sync in parallel with each other), why sanitizer builds are barely slower, and why batching matters more than predicted: with one entry per RPC, followers sync once per entry too. Leader recovery is set by the client's 500 ms request timeout, not by the election (150–300 ms): the request in flight to the dead leader has to time out first. Group commit and a shorter, adaptive client timeout are the two obvious next steps.
<!-- results:end -->

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
