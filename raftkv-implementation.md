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
(vs 3–4 in Go). The only non-C++ piece is Go itself, needed for `tools/lincheck` from
Phase 6 onward.

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
| Logging | a small logger over C++20 `std::format` | no spdlog/fmt dependency; the simulator's event log is the main debugging output anyway |
| Storage | hand-rolled **append-only segment file**, CRC32 per record, `fsync` on commit | writing it yourself is the point; RocksDB hides the failure modes |
| Concurrency | Raft core is **single-threaded** and event-driven (§3); the real node runs it on one Asio `io_context`, with `std::jthread` + `std::stop_token` for the I/O and apply threads | determinism in the simulator, structured shutdown, no detached threads |
| Errors | `tl::expected` (vcpkg `tl-expected`) — `std::expected` is C++23 | errors as values without leaving C++20 |
| Tests | **GoogleTest** + **ASan/UBSan/TSan** + **libFuzzer** on the log decoder | sanitizer-clean is the C++ equivalent of Go's `-race`. Apple clang ships no libFuzzer — fuzz on Linux CI or with Homebrew `llvm` |
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

## 3. Design decisions to fix before writing code

These are the choices the later phases depend on. Changing any of them after Phase 2 means
rewriting Raft, so they are decided here.

### 3.1 Raft is a single-threaded state machine

A simulator can only be deterministic if nothing inside it depends on OS scheduling. So the
Raft core has **no mutexes, no threads, and never blocks**. It is driven entirely from
outside, one event at a time:

```cpp
class Raft {
public:
    void on_message(const Message&);   // an RPC request or reply arrived
    void on_timer(TimerId);            // a timer it scheduled has fired
    ProposeResult propose(Command);    // client wants something in the log
};
```

and it acts on the world only through an injected `Env` (§3.2). The simulator calls these
methods from its single-threaded event loop. The real node calls them from **one thread
running the Asio `io_context`**, so the core still never sees concurrency. The only other
threads in a real node are the I/O thread (if kept separate) and the apply thread, and they
hand work to the core by `asio::post`ing to that `io_context`, never by taking a lock on
Raft state.

This removes the `std::shared_mutex` from the Raft class entirely, and with it bug #5
("holding a lock across a network call").

### 3.2 Everything nondeterministic is injected

```cpp
struct Env {
    virtual ~Env() = default;
    virtual Time now() const = 0;                          // virtual time in sim
    virtual TimerId after(Duration, TimerTag) = 0;         // fires on_timer later
    virtual void cancel(TimerId) = 0;
    virtual void send(Message) = 0;                        // Transport
    virtual Storage& storage() = 0;                        // §3.4
    virtual uint64_t random() = 0;                         // seeded PRNG in sim
    virtual void trace(std::string_view) = 0;              // a note in the event log
};
```

Rules, all checked by a CI grep over `src/raft`, `src/kv` and `src/sim`:

- no `steady_clock`, `system_clock`, `sleep_for`, `std::thread`, `std::random_device`, `rand()`;
- no `std::uniform_int_distribution` and the like. The engines (`std::mt19937_64`) give
  the same output everywhere, but the **distributions are implementation-defined**, so
  libc++ (macOS) and libstdc++ (Linux CI) turn the same seed into different election
  timeouts. Map `random()` onto a range yourself, e.g. `lo + r % (hi - lo + 1)`;
- no iteration over `unordered_map`/`unordered_set` where the order affects behaviour, and
  no ordering by pointer address.

Without these rules, `--seed N` would reproduce a failure on one machine and not on another.

### 3.3 Types and index conventions

- `using Term = uint64_t; using Index = uint64_t; using NodeId = uint32_t;` Use unsigned
  64-bit throughout, never `int`.
- The log is **1-indexed**. Index 0 is a sentinel with term 0, so `prev_log_index = 0`
  needs no special case. After compaction (Phase 8) the sentinel becomes
  `(last_included_index, last_included_term)`. From Phase 2 on, go through
  `entry_at(Index)` / `term_at(Index)` and never index `log_[i]` directly, so compaction is
  a change in one place.

### 3.4 Storage is an interface too

```cpp
class Storage {
public:
    virtual ~Storage() = default;
    virtual void save_hard_state(Term, std::optional<NodeId> voted_for) = 0; // durable on return
    virtual void append(std::span<const LogEntry>) = 0;
    virtual void truncate_suffix(Index from) = 0;   // Raft deletes conflicting entries
    virtual void sync() = 0;
    virtual PersistentState load() const = 0;       // what a restarting node reads back
};
```

`FileStorage` is Phase 4. `SimStorage` keeps an in-memory "synced" and "unsynced" copy, so
a simulated crash can **throw away unsynced writes** or tear the last record. This lets
the crash-recovery rows of the fault matrix run inside the deterministic simulator instead
of only through `kill -9`.

### 3.5 Timing constants

| Constant | Value | Note |
|---|---|---|
| Election timeout | random in 150–300 ms | re-randomized every time the timer is reset |
| Heartbeat interval | 50 ms | must be well under the minimum election timeout |
| Client request timeout | 500 ms | then retry against another node, same `(client_id, seq)` |
| Max frame size | 16 MiB | reject larger length prefixes before allocating |

In the simulator these are virtual milliseconds, so a 10-minute scenario runs in seconds.

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
    raftkvd.cpp    # one server process: raftkvd --id 1 --cluster cluster.json --data-dir data/1
                   # cluster.json maps node id -> host:port; the same file is used by every node
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
  "builtin-baseline": "<vcpkg commit sha — pin it>",
  "dependencies": ["asio", "protobuf", "gtest", "benchmark", "nlohmann-json", "tl-expected"]
}
```

Without `builtin-baseline` the versions float with whatever vcpkg checkout is present, and
"reproducible, pinned" is not true. Set it with `vcpkg x-update-baseline --add-initial-baseline`.

### 0.3 Presets that CI and you both use

The language standard and warnings go in `CMakeLists.txt`, not in preset flags, so that
every preset gets them and they apply only to project targets, not to vcpkg ports:

```cmake
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_library(raftkv_warnings INTERFACE)
target_compile_options(raftkv_warnings INTERFACE -Wall -Wextra -Wpedantic -Werror)
# every raftkv target links raftkv_warnings; generated protobuf sources do NOT
```

Protobuf-generated `.pb.cc` files do not compile cleanly under `-Wextra -Werror`. Put them in
their own library without `raftkv_warnings`, and include their headers as `SYSTEM`.

```json
{ "version": 6,
  "configurePresets": [
    { "name": "base", "hidden": true, "generator": "Ninja",
      "binaryDir": "build/${presetName}",
      "toolchainFile": "$env{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" },
    { "name": "dev",  "inherits": "base",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "Debug",
        "CMAKE_CXX_FLAGS": "-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined" } },
    { "name": "tsan", "inherits": "base",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "Debug",
        "CMAKE_CXX_FLAGS": "-fsanitize=thread" } },
    { "name": "rel",  "inherits": "base",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "RelWithDebInfo" } },
    { "name": "fuzz", "inherits": "base",
      "cacheVariables": { "CMAKE_BUILD_TYPE": "Debug", "RAFTKV_FUZZ": "ON",
        "CMAKE_CXX_FLAGS": "-fsanitize=fuzzer-no-link,address,undefined" } }
  ],
  "buildPresets": [ { "name": "dev", "configurePreset": "dev" } ],
  "testPresets":  [ { "name": "dev", "configurePreset": "dev",
                      "output": { "outputOnFailure": true },
                      "execution": { "noTestsAction": "error" } } ] }
```

`-Werror` from commit one. Turning it on later never happens. `-fno-sanitize-recover`
makes UBSan fail the test instead of printing a warning that nobody reads.

### 0.4 Toolchain notes (macOS dev, Linux CI)

- **Apple clang has no libFuzzer**: `-fsanitize=fuzzer` does not work. The `fuzz` preset
  runs in Linux CI, or locally with `brew install llvm` and
  `CC=$(brew --prefix llvm)/bin/clang`.
- `std::jthread`/`std::stop_token` need a **macOS 26 SDK or newer** with Apple's libc++;
  the 15.x SDKs do not have them.
- The Command Line Tools linker must match the default SDK. If a plain `int main(){}` fails
  with `tapi error: malformed file ... unknown architecture`, the default `MacOSX.sdk` is
  newer than the installed `ld`. Update the Command Line Tools, or pin
  `export SDKROOT=$(xcrun --sdk macosx26.5 --show-sdk-path)` in the meantime.
- **Apple clang 21 adds `-I/usr/local/include` ahead of every `-isystem` path**, so headers
  in `/usr/local/include` (often left over from an Intel Homebrew) override the vcpkg
  versions, and vcpkg port builds break too. Keep `/usr/local/include` free of anything
  this project depends on (`gtest`, `gmock`, `absl`, `google/protobuf`, `benchmark`,
  `asio`, `nlohmann`, `tl`, `fmt`). Check with
  `ls /usr/local/include`. Homebrew LLVM clang does not do this.
- Install `ninja`, `go` (for Phase 6) and vcpkg (`git clone` + `bootstrap-vcpkg.sh`, then
  `export VCPKG_ROOT=...`).

### 0.5 Repo hygiene

`.gitignore` (`build/`, `vcpkg_installed/`, `*.profraw`, fuzz corpora outside
`tests/fuzz/corpus`), `.clang-format`, and a `LICENSE`. Commit them with the skeleton.

### 0.6 CI on the first commit

Three jobs mirroring swing's pipeline, on `ubuntu-latest` with gcc-13 and clang-17:
**build+unit (ASan/UBSan)** · **TSan** · **release build + benchmark smoke test**. Also:

- the determinism lint from §3.2 (a `grep` step that fails the build);
- vcpkg binary caching (`actions/cache` on the vcpkg archive dir), or every run spends
  20+ minutes building protobuf and abseil;
- `actions/setup-go`, added now so Phase 6 does not have to touch CI setup.

A fuzz job (clang only) is added in Phase 4, once there is a decoder to fuzz.

**Done when:** `cmake --preset dev && cmake --build --preset dev && ctest --preset dev`
passes locally on macOS and in all three CI jobs, with **one** trivial GoogleTest (e.g.
`1 + 1 == 2` linked against the `raftkv` library). A zero-test run proves nothing:
it does not show that gtest, the sanitizer runtimes, or vcpkg linking actually work.

---

## Phase 1 — Deterministic simulator first

**Build this before Raft.** In Go you would lean on goroutines and the race detector; in
C++ your equivalent superpower is *determinism*: run the whole cluster in one process, on a
virtual clock, with a seeded network. Every bug becomes `--seed 8123` and reproduces
exactly. This is how FoundationDB and TigerBeetle test, and saying so in the README lands
with infra interviewers.

### 1.1 One transport interface, two implementations

```cpp
struct Message { NodeId from, to; std::string method; std::string payload; };

class Transport {
public:
    virtual ~Transport() = default;
    virtual void send(Message m) = 0;
    virtual void on_receive(std::function<void(Message)> handler) = 0;
};
```

`AsioTransport` for the real cluster; `SimTransport` for tests. Both sit behind the `Env`
of §3.2.

*As built:* there is no separate `Transport` class. `Env::send` is the transport, and the
simulator implements it directly (`src/sim/sim.cpp`). Receiving is `Node::on_message`, called
by whatever drives the node. `AsioTransport` will be the real node's `Env::send`.

### 1.2 Virtual time: a discrete-event loop, not a clock you sleep on

The simulator is a single priority queue of `(time, seq, event)`. `seq` is a
monotonically increasing tie-breaker, so two events at the same instant always run in the
same order. The loop pops the earliest event, sets `now` to its time, and runs it. Time
**jumps** to the next event; nothing ever sleeps. Message deliveries, timer firings and
scripted faults ("partition {3} at t=2s") are all just events in this queue.

```cpp
class Sim {
public:
    explicit Sim(uint64_t seed);
    void schedule(Time at, std::function<void()> fn);
    void run_until(Time t);                 // or run_until(predicate)
    Time now() const;
    uint64_t random();                      // one seeded splitmix64/mt19937_64 stream
};
```

There is no `sleep_until` anywhere. A blocking sleep cannot exist in a single-threaded
simulation, and Raft never needs one: it asks `Env::after()` for a timer and returns.
Raft code never calls `steady_clock::now()` directly. That single rule is what makes the
simulation deterministic, and the CI grep from Phase 0 enforces it.

Every event appends one line to an **event log** (`t=1234ms deliver 1->3 RequestVote term=4`).
That log is both the determinism check and the first debugging tool you reach for.

### 1.3 Fault knobs on `SimTransport`

```cpp
struct Faults {
    double drop_rate = 0.0;
    Duration delay_min{1ms}, delay_max{1ms};
    bool reorder = false;                          // false: every link is FIFO
    std::vector<std::vector<NodeId>> partitions;   // {{1,2},{3}} isolates node 3;
                                                   // a node in no group is isolated
};
```

Partitions are checked when a message is sent **and** when it arrives, so cutting a link
also drops what is in flight on it.

Plus node-level faults on `Sim` itself:

- **crash**: drop the node's in-memory state and pending timers, and keep only what
  `SimStorage` had synced (§3.4), or a random in-order prefix of the unsynced writes;
- **restart**: rebuild the node from that storage and call `on_start()`;
- **pause / resume**: the node processes nothing, like a `SIGSTOP`ped process. Messages and
  timers that arrive meanwhile are handled in arrival order on resume, not dropped.

Every seeded test honours `RAFTKV_SEED=N`, which reruns only that seed.

**Done when:**

- the same seed produces a byte-identical event log across two runs, **and across macOS and
  Linux CI** (check it with a golden-file test);
- a 30%-drop reordering network delivers the expected surviving set;
- a crashed-and-restarted `SimStorage` keeps exactly the synced writes.

---

## Phase 2 — Leader election

Implement Raft (Ongaro & Ousterhout) **Figure 2 literally**. It is a specification; deviate
only after it works.

```cpp
class Raft {                        // single-threaded: no mutex (§3.1)
    Env& env_;
    NodeId me_;
    std::vector<NodeId> peers_;
    enum class State { Follower, Candidate, Leader } state_{State::Follower};
    Term current_term_ = 0;              // persisted
    std::optional<NodeId> voted_for_;    // persisted
    std::vector<LogEntry> log_;          // persisted; log_[0] is the sentinel (§3.3)
    Index commit_index_ = 0, last_applied_ = 0;
    std::map<NodeId, Index> next_index_, match_index_;   // leader only; ordered map
};
```

Three rules that are easy to get wrong:

- Election timeouts must be **randomized** (150–300 ms) or the cluster splits votes forever.
- Any RPC carrying a **higher term** demotes you to follower and clears `voted_for_`.
- Grant a vote only if the candidate's log is **at least as up to date** as yours (last
  term, then last index). Skipping this silently loses committed entries.

**Done when:** three simulated nodes elect exactly one leader; killing the leader elects a
new one; the old leader steps down on rejoin — and all of it replays from a seed.

*As built* (`src/raft/raft.cpp`, `src/raft/messages.{proto,cpp}`):

- RPCs are plain structs (`raft/messages.hpp`); protobuf-lite is only the wire format, and
  `AppendEntries` already carries `entries` and `leader_commit` for Phase 3.
- The test harness (`tests/raft_cluster.hpp`) checks **Election Safety** (one leader per
  term), "terms never go backwards" and "one vote per term, across crashes" **after every
  simulator event**, and runs seeded chaos: random crashes, restarts, partitions, pauses
  and loss, then heals and requires a leader within 3 s. 200 seeds × {3, 5} nodes.
- Planted bugs (double voting, unpersisted vote, no log up-to-date check, keeping the vote
  on step-down) are each caught by these tests.
- `RAFTKV_SEED=N RAFTKV_DUMP_LOG=1` prints the full event log of one run.
- **No PreVote.** A node that was partitioned away comes back with an inflated term and
  forces an election. That is standard Raft (§9.6 of the thesis adds PreVote); the tests
  assert the cluster recovers, not that no election happens.

---

## Phase 3 — Log replication

`AppendEntries` with `prev_log_index`/`prev_log_term`; on rejection the leader decrements
`next_index_` and retries.

**The commit rule, read twice:** a leader may only advance `commit_index_` to an entry
**from its own term** replicated on a majority. Committing an older-term entry by majority
count alone is the most famous Raft bug (§5.4.2). Write a test for exactly that case.

Apply committed entries in order, exactly once. Raft emits `(index, command)` into an
**apply queue** as `commit_index_` advances, and never calls the state machine inline. In
the simulator the queue is drained by an event on the same loop. In the real node it is
drained by a dedicated apply thread (`std::jthread` with a `stop_token`), and results go
back to the core via `asio::post`. The state machine only ever sees entries from this one
queue, which prevents bug #6.

A follower rejecting `AppendEntries` should reply with a **conflict hint**
(`conflict_term`, `conflict_index`) so the leader can skip back a whole term instead of
one entry per round trip. Without it, the "follower partitioned for 50 entries" test is
slow, and with Phase 8's large logs it is unusable.

**Done when:** 100 entries proposed at the leader appear in identical order on all nodes,
and a follower partitioned for 50 entries catches up fully on rejoin.

*As built* (`src/raft/raft.cpp`):

- `propose()` on the leader appends and syncs, then replicates. Followers sync before
  acking. So Phase 4's "durable before replying" rule already holds against `SimStorage`,
  and a planted missing `sync()` is caught by the crash tests.
- Replies carry `match_index` explicitly, since they can arrive out of order, plus the
  `conflict_index`/`conflict_term` hint on failure. A stale reply can never move
  `match_index` back or `next_index` below it.
- A follower only truncates at a real conflict, so a delayed, shorter `AppendEntries`
  cannot delete entries that a newer one delivered.
- A new leader appends a **no-op** from its own term (paper §8). Earlier-term entries then
  commit without waiting for a client, and Phase 5's reads will need it anyway.
- Applying runs in its own zero-delay timer event, never inline, one entry at a time in
  order. `last_applied` is volatile, so a restarted node replays from index 1 until Phase 8
  adds snapshots.
- The harness now also checks **State Machine Safety**, **Leader Completeness**, "a
  committed index never changes" and in-order exactly-once apply after every event. Chaos
  runs with a client proposing every 20 ms, then requires every node to converge on one
  log after healing.
- Planted bugs (the §5.4.2 commit rule, no sync, trusting `leader_commit` past the last new
  entry, truncating matching entries, skipping the consistency check, no leader no-op) are
  all caught. The §5.4.2 bug is caught **only** by its hand-driven test: 400 chaos runs
  never produced the Figure 8 schedule, which is why that test exists.

---

## Phase 4 — Persistence and crash recovery

Persist `current_term_`, `voted_for_` and the log **before** replying to any RPC that
changed them.

**Fsync before replying, not "on commit."** Whenever a follower acks `AppendEntries`,
that ack is counted toward a majority, so the entries must already be durable. Syncing only
at commit time would let an acknowledged entry vanish in a crash. The leader must sync its
own entries before counting itself in the majority.

Two files per node:

- **`hard_state`**: `(current_term, voted_for)`. Write it to `hard_state.tmp`, `fsync`,
  `rename` over the old file, then `fsync` the directory. It is tiny and rewritten whole.
- **`log`**: append-only records `[len u32][crc32 u32][payload]`, where the payload is either
  an entry or a **truncate-from(index)** marker. Raft sometimes deletes a conflicting suffix,
  and an append-only file expresses that as a marker, not by rewriting the file. Replay
  applies the markers in order.

On macOS, `fsync` does not reach the platter. Use `fcntl(fd, F_FULLFSYNC)` there, behind
the `Storage` interface, or the `kill -9` tests pass for the wrong reason.

**Torn tail vs corruption.** These are different cases and need different handling:

| On startup the scan finds… | Meaning | Action |
|---|---|---|
| last record's `len` runs past end of file | crash mid-append; never synced, never acked | truncate it, continue |
| a complete record with a bad CRC | data that was synced (and maybe acked) is damaged | **refuse to start** |

Silently truncating a *complete* record could drop an entry this node already acknowledged,
which breaks the majority it was counted in. The fault matrix's "corrupt log tail" row tests
the second case.

```cpp
tl::expected<LogRecord, DecodeError> decode(std::span<const std::byte> in);
```

Do not throw on corrupt input, and do not `assert`. `DecodeError` distinguishes
`Truncated` from `BadChecksum`, since the table above treats them differently. Check `len`
against the max frame size before allocating.

**Fuzz this decoder with libFuzzer.** A corrupt-tail fuzzer that finds nothing after a
million cases is a genuine README line, and it is the kind of evidence C++ roles want.
Add the clang-only `fuzz` CI job here, with a time-boxed run (`-max_total_time=60`) on
every push and the corpus committed under `tests/fuzz/corpus`.

**Done when:** `kill -9` every node mid-workload, restart, and the committed prefix is
unchanged; a truncated or bit-flipped tail makes the node refuse to serve rather than serve
garbage.

*As built* (`src/store/`):

- **Record format:** `[len u32][payload_crc u32][header_crc u32][payload]`. The plan's
  `[len][crc][payload]` could not tell corruption from a torn write. A bit flip in a length
  field makes a record appear to run past end-of-file, which looks exactly like a torn write,
  so recovery would silently truncate there and drop every synced record after it. The
  header checksum makes a damaged length detectable as corruption.
- **Recovery rules** (`scan()`):
  - an incomplete last record, or zeros after the last record, is a torn tail: cut it off;
  - anything else is corruption: refuse to start.
  - A torn write that happens to leave a complete-length record with the wrong contents is
    also refused. That errs toward unavailable, never toward losing acknowledged data.
- `hard_state` is replaced atomically (tmp, fsync, rename, fsync directory) and has its
  own checksum. On macOS every sync uses `F_FULLFSYNC`.
- **Evidence:**
  - every single-bit flip in a sample log is detected;
  - every cut point recovers exactly the complete prefix;
  - a real `fork()` + `SIGKILL` test never loses an entry the child acknowledged;
  - the simulator runs the whole cluster on `FileStorage`, killing every node five times
    mid-workload. Each restart goes through recovery, with the invariant checker running
    after every event;
  - a node with a corrupted log refuses to start while the majority keeps committing;
  - libFuzzer ran 1.08M inputs in 60 s with no findings (CI fuzzes 60 s per push). A
    planted out-of-bounds read was found in seconds;
  - planted storage bugs are all caught.
- **Limit, stated plainly: `kill -9` cannot test `fsync`.** After `kill -9` the data is still
  in the page cache, so those tests pass with or without `fsync`. What shows that entries
  are synced before they are acknowledged is `SimStorage`, which does drop unsynced writes
  on a crash. Proving the disk's durability would need power-loss testing, e.g. LazyFS or
  `dm-log-writes`, which is out of scope. The simulator's file-backed tests use
  `Sync::ProcessCrashOnly` (no fsync), since they only crash processes. With fsync they
  spend 95% of their time in `F_FULLFSYNC`.
- Recovering a node whose log is corrupt needs an operator. Simply wiping its data
  directory is **not** safe in Raft: the node would forget its vote and could vote twice in
  one term.

---

## Phase 5 — KV state machine and client

Operations: `Get`, `Put`, `Append`.

**Duplicate detection is the subtle part.** A client that times out retries, so the same
command can reach the log twice. Each request carries `(client_id, seq)`; the state machine
keeps the last applied `seq` per client and returns the cached reply instead of applying
twice. Without this, `Append` silently duplicates and the linearizability check fails in a
way that is hard to read.

**Finding the leader.** A client may contact any node. A non-leader replies
`NotLeader{leader_hint}`, and the client retries there, or round-robins if there is no hint.
A leader that loses leadership before an entry commits fails the pending request with
`NotLeader`. It does not leave the request hanging. The client then retries with the
**same** `(client_id, seq)`, which is what makes the retry safe.

**The dedup table is state-machine state.** Update it only when applying the entry, never
when receiving the request, so every replica has an identical table. It goes into
snapshots in Phase 8. `Get`s do not need dedup, only `Put`/`Append`. One outstanding
request per client keeps "last seq per client" sufficient. Evicting sessions of clients
that have gone away is out of scope for v1; state that in the README.

Reads go through the log at first (simple, linearizable). If you later add `ReadIndex` or
lease reads, **state the consistency you now provide** and measure the latency difference —
that comparison is a good write-up section.

**Done when:** a client survives leader failover mid-request with no duplicated `Append`.

*As built, part 1* (`src/kv/`):

- `StateMachine` deduplicates **every** op by `(client_id, seq)`, `Get` included. That is
  simpler than special-casing reads, and a retried `Get` gets the reply the original saw.
- `Server` wraps `Raft` and proposes each request. It replies when the entry applies, and
  fails pending requests with `NotLeader` once deposed. It also checks, as a backstop, that
  the entry applied at a pending index is really that request. Each of these alone keeps
  replies correct; removing both lets chaos tell a client "OK" for a lost append.
- `Client` follows `NotLeader` hints, backs off 20 ms when there is no hint, moves to the
  next server after a 500 ms timeout, and always retries with the same seq.
- The done-gate test kills the leader in the instant between **commit and apply**, so no
  reply is ever sent and the retry hits an entry that is already committed. It runs 10
  times × 20 seeds. Chaos with 3 clients × 50 seeds checks that each client's appends land
  exactly once and in order.
- Planted bugs (no dedup, dedup not keyed by client, retry with a new seq) are all caught.
- It found a bug in the test harness: see `docs/postmortems/001-leader-completeness-check-too-strict.md`.

**Also in this phase: the real server.** No earlier phase builds it: the Asio transport
(length-prefixed frames behind `Env::send`), and `raftkvd`/`raftkvctl` running `Raft` +
`FileStorage` as separate processes. Phase 4's cluster-wide "`kill -9` every node" runs in
the simulator until then. The real-process version belongs to Phase 7's process suite.

---

## Phase 6 — Prove it: linearizability checking

The phase that makes this more than a demo.

Record every operation as `{client, op, key, value, invoke_ns, return_ns}` and export JSON.
In the simulator the timestamps are virtual time. In the real cluster they come from one
monotonic clock on the client host, never from the servers.

**Operations that time out are not failures.** A `Put` whose reply never arrived may or may
not have been applied. Record it with no return, or `return_ns = +∞`, so Porcupine treats
it as possibly taking effect at any later point. Dropping it from the history, or marking
it failed, makes the checker report false violations, or worse, miss real ones.
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
| Torn log tail | crash mid-append (partial last record) | truncates the unsynced record, rejoins, catches up |
| Corrupt log tail | flip bytes in a complete, synced record | refuses to start; no stale reads |

**Where each row runs.** Every row runs in the **simulator** under a seed. That is the
authoritative, reproducible version. A smaller **real-process** suite
(`tests/cluster/*.sh`, or a GoogleTest fixture that spawns `raftkvd`) repeats the leader
crash, follower `kill -9` and corrupt-tail rows against real sockets and real `fsync`.
That shows the Asio transport and `FileStorage` behave like their simulated versions.
The real-process runs are not seed-replayable, so the README says which rows ran where.

**Done when:** every row passes under `--seed` replay, at `ctest --repeat until-fail:10`,
with ASan/UBSan and TSan both clean.

---

## Phase 8 — Snapshots and compaction

Without compaction the log grows forever and restarts slow down.

- State machine serializes its state, **including the dedup table** from Phase 5. Without
  it, a follower restored from a snapshot re-applies a retried `Append`. Raft discards
  entries up to that index.
- Snapshot files are written like `hard_state`: temp file, `fsync`, `rename`, `fsync` the
  directory. Only after that does the log get truncated.
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
4. Persisting **after** replying instead of before, or `fsync`ing only at commit.
5. Holding a lock across a network call — deadlock, or deciding on stale state.
6. Applying entries twice or out of order because the apply loop races the commit index.
7. Using a pre-compaction index into the log vector after snapshotting.

## C++-specific traps

8. Calling `steady_clock::now()` instead of the injected `Clock` — breaks determinism.
9. Detached threads instead of `std::jthread` — shutdown hangs and TSan noise.
10. Dangling `std::span`/`string_view` into a buffer the transport already recycled.
11. `shared_ptr` cycles between node and transport — use `weak_ptr` for the back-reference.
12. Throwing across the Asio callback boundary — handle errors as values.
13. `std::uniform_int_distribution` in simulated code — the same seed gives different
    numbers on libc++ and libstdc++, so replays break across machines (§3.2).
14. Trusting `fsync` on macOS — use `F_FULLFSYNC` (Phase 4).

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
