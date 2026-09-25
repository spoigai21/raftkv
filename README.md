# raftkv

A replicated key–value store in C++20, built on the Raft consensus algorithm.
It runs as 3 or 5 processes on one machine, talking over real sockets, and keeps working when a minority of them crash.
It is a learning project: one Raft group, no transactions or indexes, and it isn't built to be fast.

> **Status:** Phase 1 — deterministic simulator. No Raft code yet.

## What it will do

- **Replicate:** every node applies the same writes in the same order.
- **Survive failures:** if the leader dies, the others elect a new one, and no acknowledged write is lost.
- **Fail safely:** a node cut off from the majority refuses writes instead of returning wrong answers.
- **Recover from crashes:** state is saved to disk with checksums, so a node can be killed with `kill -9` and restart cleanly.
- **Serve a simple KV API:** `Get`, `Put` and `Append`, with duplicate detection so a retried request is applied only once.
- **Compact its log:** snapshots keep the on-disk log from growing forever.

## How it will be tested

- **Deterministic simulator:** the whole cluster runs in one process on a virtual clock with a seeded network, so any failure replays exactly from `--seed N`.
- **Fault matrix:** crashes, partitions, message drops, delays and corrupted logs are all injected by automated tests.
- **Linearizability checking:** recorded operation histories are checked with [Porcupine](https://github.com/anishathalye/porcupine).
- **Memory and thread safety:** CI runs ASan, UBSan and TSan, and the log decoder is fuzzed with libFuzzer.
- **Benchmarks:** throughput, latency and failover time, written to `docs/results.csv`.

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

## Out of scope for v1

Membership changes, `ReadIndex`/lease reads, a gRPC front end, and sharding across multiple Raft groups.

## More detail

- [`raftkv.md`](raftkv.md): goals, scope and what counts as done
- [`raftkv-implementation.md`](raftkv-implementation.md): the phase-by-phase build plan
