# raftkv — Implementation Guide (C++20)

A replicated key–value store built on the Raft consensus algorithm, in modern C++20.
Three or five nodes, real sockets, real crashes, a deterministic simulator that can replay
any failure from a seed, and a harness that *proves* no acknowledged write is ever lost.

Named for what it is: a key-value store on Raft. A *quorum* - the majority of nodes that must
agree before anything is committed - is the idea the whole system turns on.

---

## 1. Motivation — why this project, and why in C++

### Gap 1: distributed systems

Across this application cycle one requirement keeps appearing that nothing on the résumé
answers:

| Company | What the JD asked for | What the résumé could offer |
|---|---|---|
| Qumulo | "fault-tolerant block layer that erasure-codes and distributes data"; "a distributed operating system problem: scheduling, memory hierarchy, protection, concurrency" | application backends |
| Databricks | infrastructure, systems, databases, "a platform that scales" | single-node data pipelines |
| Cisco (backend) | "RESTful services and **distributed architectures**" | REST, nothing distributed |
| RTX / HII | large-scale systems | Java monolith |

Every project on the record — Adorus, HerbsPro, Countera, swing, CineInfer — runs on one
machine. They prove product engineering, data handling and measurement discipline. None
proves what infra teams screen for: **what your system does when part of it dies.**

### Gap 2: modern C++ (the narrower, accurate version)

You are not missing C++ — the record has the CUDA softmax kernels (2,818 lines, 64 files),
a Qt desktop app, ~1,948 lines of CSES algorithm solutions, and a year of TA'ing C++ labs.
What is missing is **systems-grade modern C++**: no C++20 features, no CMake/toolchain
discipline, no sanitizer or fuzzing evidence, no concurrency outside GPU kernels.

That matters because **C++ appears in 22 of the 65 saved JDs** — more than Go (15) or Rust
(5) — and it is the language of the firms where the rest of your record already points
(Qumulo, Databricks, Optiver, Nutanix, SingleStore, Rubrik, Tower).

Building this in C++20 rather than Go does three things at once: it closes the distributed
gap, it converts "C++ coursework and CUDA kernels" into "C++ systems engineer", and it
pairs with the GPU work to make C++ your unambiguous systems language.

**Cost of choosing C++ over Go:** roughly 40–60% more time. Go gives you goroutines, a
built-in race detector and `net/rpc` free; in C++ you build the event loop, the framing and
the fault injection yourself. That extra work is itself the differentiator — see §3.

### What a reader learns about you

- You implemented a **consensus protocol from the paper**, not imported one.
- You understand **replication, crash recovery, compaction** and **concurrency** — and you
  can prove memory and thread safety with sanitizers rather than assert it.
- You can drive a real **C++20 toolchain**: CMake presets, vcpkg, cross-compiler CI.
- You verify with **adversarial, reproducible tests**, consistent with how you already work
  on swing (561 tests, 3-job CI, 54 postmortems).

### Honest scope — state these limits in the README

- Single machine, multiple processes. "Distributed" means independent processes with real
  sockets and injectable faults, not geographic distribution.
- One Raft group. No sharding, no membership changes unless you build Phase 10's stretch.
- Not a database: no transactions, no secondary indexes, no SQL.
- Benchmarks describe *this laptop* and exist to compare configurations, not production
  systems.

### Cost

**$0.** Compiler, CMake, vcpkg, Asio, GoogleTest, sanitizers, Porcupine — all free. The
cluster is processes on your laptop. Budget **5–7 weeks part-time** to a demoable Phase 8
(vs 3–4 in Go).

### On MIT 6.5840

Use the course to *learn* Raft — the lectures and paper walkthrough are the best free
material there is. The labs are Go; you are writing C++, so you are re-implementing rather
than copying, which sidesteps the "don't publish lab solutions" problem entirely and leaves
you with a repo that isn't the thousandth `6.5840-raft` on GitHub.

---

## 2. The stack

| Layer | Choice | Why |
|---|---|---|
| Language | **C++20** (gcc 13+ / clang 17+) | concepts, ranges, `std::span`, `std::jthread`, `<format>`, designated initializers |
| Build | **CMake ≥3.25** + `CMakePresets.json` + **Ninja** | presets make the CI matrix and the local build identical |
| Dependencies | **vcpkg** (manifest mode, `vcpkg.json`) | reproducible, pinned, no vendored source |
| Transport | **standalone Asio** (non-Boost) + length-prefixed framing | you own the socket layer, which is where fault injection lives |
| Encoding | **Protocol Buffers** (protobuf-lite) | versioned messages; `nlohmann::json` for history dumps only |
| Storage | hand-rolled **append-only segment file**, CRC32 per record, `fsync` on commit | writing it yourself is the point; RocksDB hides the failure modes |
| Concurrency | `std::jthread` + `std::stop_token`, `std::atomic`, `std::shared_mutex`, one event loop per node | structured shutdown, no detached threads |
| Tests | **GoogleTest** + **ASan/UBSan/TSan** + **libFuzzer** on the log decoder | sanitizer-clean is the C++ equivalent of Go's `-race` |
| Simulation | own deterministic simulator: virtual clock + seeded PRNG network | replay any failure from a seed — the standout feature |
| Verification | **Porcupine** (Go) run over exported JSON histories, via `tools/lincheck` | pragmatic: a 40-line Go dev tool, documented as such |
| Benchmarks | **Google Benchmark** | stable numbers for the results table |
| CI | GitHub Actions: gcc-13 + clang-17 × {Debug+ASan/UBSan, TSan, Release} | three jobs, same shape as swing's pipeline |
| Repro | Docker image pinning the toolchain | you already did this for the CUDA project |

**Why Asio over gRPC:** gRPC would work and is industry-standard, but it owns the transport,
which is exactly the layer you need to corrupt, delay and partition. Hand-rolled framing
over Asio keeps fault injection in your own code and teaches more. If you later want gRPC
on your résumé, add a gRPC front end in Phase 10 and keep Asio underneath.

---

## Phase 0 — Setup

### 0.1 Skeleton

```
raftkv/
  CMakeLists.txt  CMakePresets.json  vcpkg.json
  src/
    raft/        # consensus: election, replication, persistence, snapshots
    kv/          # state machine: Get/Put/Append + duplicate detection
    net/         # Asio transport, framing, fault injection
    sim/         # deterministic simulator: virtual clock + seeded network
    store/       # append-only log, CRC, fsync, recovery
    client/
  apps/
    raftkvd.cpp    # one server process
    raftkvctl.cpp  # CLI client
  tests/         # GoogleTest
  bench/         # Google Benchmark
  tools/lincheck # small Go program wrapping Porcupine
  docs/          # design notes, results.csv, postmortems
```

### 0.2 vcpkg manifest

```json
{
  "name": "raftkv",
  "dependencies": ["asio", "protobuf", "gtest", "benchmark", "nlohmann-json", "spdlog"]
}
```

### 0.3 Presets that CI and you both use

```json
{ "version": 6,
  "configurePresets": [
    { "name": "dev",  "generator": "Ninja", "binaryDir": "build/dev",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "Debug",
        "CMAKE_CXX_FLAGS": "-std=c++20 -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined -fno-omit-frame-pointer" } },
    { "name": "tsan", "generator": "Ninja", "binaryDir": "build/tsan",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "Debug",
        "CMAKE_CXX_FLAGS": "-std=c++20 -fsanitize=thread" } },
    { "name": "rel",  "generator": "Ninja", "binaryDir": "build/rel",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "RelWithDebInfo" } }
  ] }
```

`-Werror` from commit one. Turning it on later never happens.

### 0.4 CI on the first commit

Three jobs mirroring swing's pipeline: **build+unit (ASan/UBSan)** · **TSan** ·
**release build + benchmark smoke test**. Green on an empty repo before there is code to
break.

**Done when:** `cmake --preset dev && ninja -C build/dev && ctest` runs with zero tests, and
CI is green.

---

## Phase 1 — Deterministic simulator first

**Build this before Raft.** In Go you would lean on goroutines and the race detector; in
C++ your equivalent superpower is *determinism*: run the whole cluster in one process, on a
virtual clock, with a seeded network. Every bug becomes `--seed 8123` and reproduces
exactly. This is how FoundationDB and TigerBeetle test, and saying so in the README lands
with infra interviewers.

### 1.1 One transport interface, two implementations

```cpp
struct Message { int from, to; std::string method; std::string payload; };

class Transport {
public:
    virtual ~Transport() = default;
    virtual void send(Message m) = 0;
    virtual void on_receive(std::function<void(Message)> handler) = 0;
};
```

`AsioTransport` for the real cluster; `SimTransport` for tests.

### 1.2 Virtual clock

```cpp
class Clock {                       // SimClock advances only when the queue is drained
public:
    virtual std::chrono::steady_clock::time_point now() const = 0;
    virtual void sleep_until(std::chrono::steady_clock::time_point) = 0;
};
```

Raft code takes a `Clock&` and never calls `steady_clock::now()` directly. That single rule
is what makes the simulation deterministic — grep for violations in CI.

### 1.3 Fault knobs on `SimTransport`

```cpp
struct Faults {
    double drop_rate = 0.0;
    std::chrono::milliseconds delay_min{0}, delay_max{0};
    bool reorder = false;
    std::vector<std::vector<int>> partitions;  // {{1,2},{3}} isolates node 3
    std::set<int> paused;                      // node processes nothing
};
```

**Done when:** the same seed produces a byte-identical event log across two runs, and a
30%-drop reordering network delivers the expected surviving set.

---

## Phase 2 — Leader election

Implement Raft (Ongaro & Ousterhout) **Figure 2 literally**. It is a specification; deviate
only after it works.

```cpp
class Raft {
    std::shared_mutex mu_;
    int me_;
    std::vector<int> peers_;
    enum class State { Follower, Candidate, Leader } state_{State::Follower};
    int current_term_ = 0;          // persisted
    std::optional<int> voted_for_;  // persisted
    std::vector<LogEntry> log_;     // persisted
    int commit_index_ = 0, last_applied_ = 0;
    std::vector<int> next_index_, match_index_;   // leader only
};
```

Three rules that are easy to get wrong:

- Election timeouts must be **randomized** (150–300 ms) or the cluster splits votes forever.
- Any RPC carrying a **higher term** demotes you to follower and clears `voted_for_`.
- Grant a vote only if the candidate's log is **at least as up to date** as yours (last
  term, then last index). Skipping this silently loses committed entries.

**Done when:** three simulated nodes elect exactly one leader; killing the leader elects a
new one; the old leader steps down on rejoin — and all of it replays from a seed.

---

## Phase 3 — Log replication

`AppendEntries` with `prev_log_index`/`prev_log_term`; on rejection the leader decrements
`next_index_` and retries.

**The commit rule, read twice:** a leader may only advance `commit_index_` to an entry
**from its own term** replicated on a majority. Committing an older-term entry by majority
count alone is the most famous Raft bug (§5.4.2). Write a test for exactly that case.

Apply committed entries in order, exactly once, on a dedicated apply thread
(`std::jthread` with a `stop_token`).

**Done when:** 100 entries proposed at the leader appear in identical order on all nodes,
and a follower partitioned for 50 entries catches up fully on rejoin.

---

## Phase 4 — Persistence and crash recovery

Persist `current_term_`, `voted_for_` and the log **before** replying to any RPC that
changed them.

Record format: `[len u32][crc32 u32][payload]`, appended, `fsync` on commit. On startup,
scan forward, stop at the first bad CRC, truncate there.

```cpp
std::expected<LogRecord, DecodeError> decode(std::span<const std::byte> in);
```

`std::expected` (C++23) or a `tl::expected`/`std::variant` equivalent — do not throw on
corrupt input, and do not `assert`.

**Fuzz this decoder with libFuzzer.** A corrupt-tail fuzzer that finds nothing after a
million cases is a genuine README line, and it is the kind of evidence C++ roles want.

**Done when:** `kill -9` every node mid-workload, restart, and the committed prefix is
unchanged; a truncated or bit-flipped tail makes the node refuse to serve rather than serve
garbage.

---

## Phase 5 — KV state machine and client

Operations: `Get`, `Put`, `Append`.

**Duplicate detection is the subtle part.** A client that times out retries, so the same
command can reach the log twice. Each request carries `(client_id, seq)`; the state machine
keeps the last applied `seq` per client and returns the cached reply instead of applying
twice. Without this, `Append` silently duplicates and the linearizability check fails in a
way that is hard to read.

Reads go through the log at first (simple, linearizable). If you later add `ReadIndex` or
lease reads, **state the consistency you now provide** and measure the latency difference —
that comparison is a good write-up section.

**Done when:** a client survives leader failover mid-request with no duplicated `Append`.

---

## Phase 6 — Prove it: linearizability checking

The phase that makes this more than a demo.

Record every operation as `{client, op, key, value, invoke_ns, return_ns}` and export JSON.
Then check it with Porcupine through a small Go tool:

```go
// tools/lincheck/main.go — dev dependency, documented as such in the README
res, info := porcupine.CheckOperationsVerbose(kvModel, history, 10*time.Second)
if res != porcupine.Ok {
    porcupine.VisualizePath(kvModel, info, "docs/failure.html")
    os.Exit(1)
}
```

Using a Go tool from a C++ project is a defensible engineering choice, not a cheat — say
plainly in the README that history checking runs offline in a separate tool. The
alternative (porting the WGL algorithm to C++) is a fine stretch goal but not the point.

**Done when:** 100 randomized seeds with concurrent clients and injected faults all pass,
and `docs/` contains at least one saved visualization of a real bug you found and fixed.

---

## Phase 7 — The fault matrix

Each row is an automated test, not a manual demo. This table is the centrepiece of the
README.

| Scenario | Injection | Expected |
|---|---|---|
| Leader crash mid-write | kill after `AppendEntries` sent, before commit | new leader ≈1 s; no acknowledged write lost |
| Minority partition | isolate 1 of 3 | majority serves; minority refuses writes |
| Majority partition | isolate 2 of 3 | **unavailable, not inconsistent** |
| Follower crash + restart | `kill -9`, restart | catches up from log or snapshot |
| Slow follower | 500 ms delay on one link | cluster available; p50 unchanged |
| Message drops | 20% drop | progress continues via retries |
| Paused leader | pause node | followers elect; old leader steps down on resume |
| Corrupt log tail | flip bytes | refuses to start; no stale reads |

**Done when:** every row passes under `--seed` replay, at `ctest --repeat until-fail:10`,
with ASan/UBSan and TSan both clean.

---

## Phase 8 — Snapshots and compaction

Without compaction the log grows forever and restarts slow down.

- State machine serializes its state; Raft discards entries up to that index.
- Leader sends `InstallSnapshot` to a follower too far behind.
- `last_included_index` / `last_included_term` bookkeeping is where off-by-ones live —
  especially converting absolute indices to positions in a compacted `std::vector`.

**Done when:** after 100k operations the on-disk log is bounded, and a fresh node catches up
by snapshot in under a second.

---

## Phase 9 — Measure it

Report the machine. Write results from code to `docs/results.csv`; the README reads from it
so the write-up cannot drift (the `--check` pattern from swing).

| Metric | Why it matters |
|---|---|
| Throughput vs cluster size (1/3/5) | the price of replication |
| Latency p50 / p99 | reads vs writes, log-based |
| **Recovery time after leader kill** | the headline number |
| Throughput during minority partition | degrades, not stops |
| Batching effect (1 entry/RPC vs batched) | usually several × |
| Snapshot pause vs state size | compaction cost |
| Debug vs Release vs sanitizer builds | honest about measurement conditions |

### Predictions, committed before measuring

1. Write throughput drops ___% from 3 to 5 nodes.
2. Recovery after leader kill is under ___ ms.
3. Batching improves throughput ___×.
4. A 20% drop rate costs less than ___% throughput.

---

## Phase 10 — Write-up and demo

README order: what it is (3 sentences, scope limits up front) · architecture diagram ·
**fault matrix with pass/fail** · benchmark table with the machine named · **what I got
wrong** (the bugs, especially any the linearizability checker caught — your swing
postmortems are the model) · limits and next steps.

**Demo, 2–4 minutes, narrated:** one command starts a 3-node cluster · a client writes
continuously · **you kill the leader on camera** · writes pause and resume · the checker
reports the history linearizable · one limitation stated out loud.

**Stretch, in order of value:** membership changes (add/remove a node) → `ReadIndex` reads →
a gRPC front end → multi-Raft sharding.

---

## Common Raft bugs — check these first

1. Committing a **previous-term** entry by majority count alone (§5.4.2).
2. Missing the **up-to-date log check** when granting a vote.
3. Not resetting the election timer on a *valid* `AppendEntries` from the current leader.
4. Persisting **after** replying instead of before.
5. Holding a lock across a network call — deadlock, or deciding on stale state.
6. Applying entries twice or out of order because the apply loop races the commit index.
7. Using a pre-compaction index into the log vector after snapshotting.

## C++-specific traps

8. Calling `steady_clock::now()` instead of the injected `Clock` — breaks determinism.
9. Detached threads instead of `std::jthread` — shutdown hangs and TSan noise.
10. Dangling `std::span`/`string_view` into a buffer the transport already recycled.
11. `shared_ptr` cycles between node and transport — use `weak_ptr` for the back-reference.
12. Throwing across the Asio callback boundary — handle errors as values.

---

## What this produces for the résumé

Written only once the numbers exist — no placeholders:

- *"Implemented Raft consensus from the paper in **C++20** (N lines, M tests): leader
  election, log replication, crash recovery, snapshotting — verified linearizable by
  Porcupine across 100 seeded fault runs, zero acknowledged writes lost."*
- *"Built a deterministic simulator (virtual clock, seeded network) so every failure
  replays from a seed; ASan/UBSan/TSan clean in CI and the log decoder fuzzed to N million
  cases."*
- *"Measured failover: a 3-node cluster resumed writes ___ ms after the leader was killed;
  throughput fell ___% at 5 nodes, and a minority partition degraded rather than stopped
  service."*

All three are claims about **failure behavior and safety evidence** — what the C++ and infra
postings screen for, and what nothing else on the record currently says.

---

## Appendix — if you ever want the Go version instead

Go would cut this to 3–4 weeks: goroutines and channels fit Raft naturally, `net/rpc` is
built in, the race detector is free, Porcupine is native, and the 6.5840 materials line up
directly. It closes the Go gap (15 of 65 JDs) instead of the modern-C++ gap (22 of 65).
The phase structure above is unchanged; only §2's stack row and the code sketches differ.
Pick C++20 if you want one systems language told well, alongside the CUDA work.
