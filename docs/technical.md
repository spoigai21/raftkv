# raftkv: the technical details

The [README](../README.md) explains raftkv in plain language. This page is the detail behind
it: the architecture, how it is tested, the fault matrix, what went wrong, and the limits.
The measurements are in [`results.md`](results.md), generated from the CSV files.

## Architecture

```mermaid
flowchart LR
    client["raftkvctl / raftkvload<br/>kv::Client"] -- "KvRequest / KvReply" --> server
    subgraph server["raftkvd (one per node)"]
        direction TB
        kvserver["kv::Server<br/>state machine + dedup table"] --> raft["raft::Raft<br/>single-threaded, event-driven"]
        raft --> env["Env: time, timers, send, storage, randomness"]
    end
    env -- "real process" --> asio["AsioEnv<br/>TCP frames, steady_timer"]
    env -- "real process" --> file["FileStorage<br/>log, hard_state, snapshot"]
    env -. "in tests" .-> sim["Sim<br/>virtual clock, seeded network,<br/>crashes, partitions, pauses"]
    env -. "in tests" .-> simstore["SimStorage<br/>loses unsynced writes on crash"]
```

- **The Raft core** (`src/raft/raft.cpp`) never blocks, never takes a lock and never asks
  the OS for anything. It is driven one event at a time, and everything nondeterministic
  comes through one `Env` interface. Tests give it the simulator, so a whole cluster runs in
  one process on a virtual clock and any failure replays from `RAFTKV_SEED=N`. `raftkvd`
  gives it Asio and real files. The same `Raft` and `kv::Server` code runs in both.
- **What Raft does here:** leader election with **PreVote** and **CheckQuorum**; log
  replication with the §5.4.2 commit rule and conflict hints; **group commit** (one fsync
  for every proposal that arrived since the last); snapshots and `InstallSnapshot`.
- **Storage** (`src/store/`): an append-only log of records with their own header checksum,
  plus `hard_state` and `snapshot` files replaced atomically (temp file, fsync, rename,
  fsync the directory). A torn last record is cut off; any other damage makes the node
  refuse to start.
- **The KV layer** (`src/kv/`): `Get`/`Put`/`Append`, all through the log. A per-client
  `(client_id, seq)` dedup table, also kept in snapshots, applies each request once. The
  client follows leader hints and adapts its timeout to measured round trips (RFC 6298),
  resending once to the same server before moving on.
- **The real server** (`src/net/`, `apps/`): one thread per process running an Asio
  `io_context`, with length-prefixed protobuf frames over TCP.

## How it is tested

- **Deterministic simulator.** It covers a seeded network, crashes, partitions, pauses,
  message loss and slow links. Raft's safety properties are checked after **every**
  simulated event:
  - one leader per term;
  - Leader Completeness;
  - committed entries never change;
  - State Machine Safety.

  Golden tests check that a seed replays byte-identically on macOS and Linux.
- **Fault matrix.** Every row below is an automated test.
- **Linearizability.** Client histories are checked with
  [Porcupine](https://github.com/anishathalye/porcupine), via `tools/lincheck`, a small Go
  dev tool:
  - 540 simulator histories, about 361k operations;
  - a real-process history that spans a `kill -9` of the leader.

  All are linearizable. Planted bugs (stale local reads, no dedup, replying before commit,
  dedup missing from snapshots) are all caught; see [`lincheck/`](lincheck/).
- **Real processes.** `tests/cluster/smoke.sh` and `tools/demo.sh --check` run `raftkvd`
  for real and `kill -9` it. CI repeats the smoke test 10× per job.
- **Memory and thread safety.** ASan/UBSan and TSan run in CI, with a hardened standard
  library. The on-disk log decoder is fuzzed with libFuzzer: over a million inputs a minute,
  and CI fuzzes every push.
- **Mutation checks.** Each phase planted the bugs its tests exist for, and confirmed the
  tests fail. Where a planted bug went undetected, a test was added for it.

## Fault matrix

Every row is an automated test (`tests/fault_matrix_test.cpp`). It runs under 20 seeds
(any one replays with `RAFTKV_SEED=N`), with 3 servers and 3 clients doing get/put/append.
The Raft invariants are checked after every event, and every run's history goes through
the linearizability checker, which is what "no acknowledged write lost" means here. Rows
marked in the last column also run against real `raftkvd` processes.

| Scenario | Injection | Expected | Measured | Simulator test | Real processes |
|---|---|---|---|---|---|
| Leader crash mid-write | kill the leader after it sent `AppendEntries`, before commit | new leader ≈1 s; no acknowledged write lost | new leader in 224 ms median, 499 ms worst | `LeaderCrashMidWrite` | ✓ every node `kill -9`'d mid-write |
| Minority partition | cut 1 of 3 off (leader or follower) | majority serves; minority refuses writes | the cut-off node commits nothing; a cut-off leader steps down (CheckQuorum) | `MinorityPartition` | |
| Majority partition | cut every server off from every other | **unavailable, not inconsistent** | nothing commits, no op completes; consistent after healing | `MajorityPartition` | |
| Follower crash + restart | `kill -9` a follower, restart 1 s later | catches up | recovers from its own log, then catches up | `FollowerCrashAndRestart` | ✓ all nodes `kill -9` |
| Slow follower: slow replies | +500 ms on its replies to the leader | available; p50 unchanged | available; p50 **+11%**, throughput −10% (median of 20 seeds). Commits wait for the one fast follower instead of the faster of two | `SlowFollowerReplies` | |
| Slow follower: slow link both ways | +500 ms each way | available; p50 unchanged | available; p50 +10%, throughput −10%; **no extra elections** (PreVote). Before PreVote: 98 forced elections and up to −22% | `SlowFollowerLinkBothWays` | |
| Message drops | 20% of all messages, 5 s | progress continues via retries | every client progresses, at **18.5%** of normal throughput (5.2% before the adaptive client timeout) | `TwentyPercentMessageDrops` | |
| Paused leader | `SIGSTOP` the leader for 1 s | followers elect; old leader steps down on resume | as expected | `PausedLeader` | |
| Torn log tail | half a record at the end of a follower's log | cuts it off, rejoins, catches up | as expected | `TornLogTail` | |
| Corrupt log tail | flip a byte in a complete, synced record | refuses to start; no stale reads | refuses to start; the other two keep serving | `CorruptLogTail` | ✓ (log or snapshot) |

Two rows still differ from the plan's expectation, and the table reports them as measured
rather than with the thresholds adjusted until they passed. With one slow follower, a
3-node cluster loses the "faster of two followers" effect, so p50 rises by about 10%. And
20% loss still costs most of the throughput: each lost message costs the client at least
one timeout.

## What went wrong

Everything here was found by the tests above, and each is written up.

- **A server could crash when a peer died mid-write**
  ([postmortem 002](postmortems/002-pop-from-emptied-write-queue.md)). It only shows up
  with real sockets, which the simulator never uses. The repeated real-process test caught
  it (5 of 20 Release runs crashed). A hardened standard library now traps it at the exact line.
- **The invariant checker was stricter than the paper**
  ([postmortem 001](postmortems/001-leader-completeness-check-too-strict.md)). A node
  paused mid-election legitimately led an old term. The fix was to state Leader
  Completeness exactly as Figure 3 does.
- **The first on-disk record format** (`[len][crc][payload]`) could not tell a damaged
  length from a torn write, so recovery could silently drop synced records. Caught in design
  review, before any data was written; records now carry a header checksum.
- **Under load with no faults, the cluster held needless elections.** The leader blocked
  its one thread for a 4 ms fsync per request, long enough with 32 clients to miss
  heartbeats. A faster client timeout made it worse, by resending, which meant more
  proposals and more fsyncs. Group commit fixed the cause: one fsync per batch.
- **The first adaptive client timeout made failover slower** (836–1,031 ms). It carried a
  doubled timeout from server to server. Caught by measuring real processes, not only the
  simulator, and fixed.
- **Two of three benchmark predictions were wrong** (see [`results.md`](results.md)): five
  nodes cost 7.3% rather than 25%, and batching gave 3.4× rather than 2×. Both come from
  not yet appreciating how completely fsync dominated.
- **Test bugs:** a deadline-less test that could hang; a smoke test that "corrupted" an
  empty file, which was really a torn tail; and guessed thresholds that the data
  contradicted. Each was found and fixed, with the reason in the commit history.
- **The checker has not found a real bug** that the other tests missed. That was prediction 5;
  so far it is wrong. It has caught every planted one.

## Limits and next steps

- **fsync still sets the pace.** Group commit shares one 4 ms `F_FULLFSYNC` across a batch,
  but every batch pays one. Without fsync the cluster is about 10× faster; see
  [`results.md`](results.md).
- **Group commit costs the unsafe no-fsync mode.** The flush runs as its own zero-delay timer
  event, which adds about 1.3 ms per batch. With a 4 ms fsync that is a bargain (4.7× more
  durable writes), but with no fsync it is pure overhead: 70k → 16k ops/s in a quick
  comparison. Flushing immediately when nothing else is queued, or posting the flush
  instead of using a timer, would likely recover it. Only the measurement-only
  `--unsafe-no-fsync` mode is affected.
- **Measured on battery.** The final measurements ran on battery power, which macOS may
  slow down; Phase 9's power source was not recorded. `docs/results.csv` now records it.
- **Spurious client resends:** under load about 1–2% of requests are resent, because the
  100 ms timeout floor sits near p99. Dedup makes them harmless, but they are wasted work.
- **Snapshots travel in one message**, so the state must fit a 16 MiB frame.
- **Not tested: power loss.** `kill -9` cannot show that fsync is durable. `SimStorage`
  models lost unsynced writes; the disk itself would need LazyFS or `dm-log-writes`.
- **Out of scope for v1:** membership changes, `ReadIndex`/lease reads, a gRPC front end,
  multi-Raft sharding, client-session eviction.
- **Open question:** once, before snapshots existed, the smoke test's corruption step found
  its byte missing from the file afterwards. It has not recurred in the 300-odd runs since;
  the test now verifies the corruption and fails fast with logs if it happens again.
