// Micro-benchmarks for the pieces whose cost the cluster benchmarks cannot separate out
// (implementation guide Phase 9): checksums, the log codec, the state machine, snapshots of
// growing state, and what fsync costs on this machine.

#include <benchmark/benchmark.h>

#include <unistd.h>

#include <filesystem>
#include <string>
#include <vector>

#include "kv/state_machine.hpp"
#include "store/file_storage.hpp"
#include "store/log_codec.hpp"

namespace {

using namespace raftkv;

// A fresh directory per benchmark run, removed afterwards.
struct ScratchDir {
    ScratchDir() : path(std::filesystem::temp_directory_path() / ("raftkv-bench-" + std::to_string(::getpid()))) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
    }
    ~ScratchDir() { std::filesystem::remove_all(path); }
    std::filesystem::path path;
};

kv::StateMachine state_with(std::int64_t keys) {
    kv::StateMachine sm;
    for (std::int64_t i = 0; i < keys; ++i) {
        sm.apply({.client_id = static_cast<std::uint64_t>(i % 64) + 1, .seq = static_cast<std::uint64_t>(i) + 1,
                  .op = kv::Op::Put, .key = "key" + std::to_string(i), .value = std::string(16, 'v')});
    }
    return sm;
}

void BM_Crc32(benchmark::State& state) {
    const std::vector<std::byte> data(static_cast<std::size_t>(state.range(0)), std::byte{0x5a});
    for (auto _ : state) benchmark::DoNotOptimize(store::crc32(data));
    state.SetBytesProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_Crc32)->Arg(4096);

void BM_LogRecordEncodeDecode(benchmark::State& state) {
    const raft::LogEntry e{.term = 7, .index = 123456, .command = std::string(64, 'c')};
    std::vector<std::byte> buf;
    for (auto _ : state) {
        buf.clear();
        store::encode(e, buf);
        benchmark::DoNotOptimize(store::decode(buf));
    }
}
BENCHMARK(BM_LogRecordEncodeDecode);

void BM_KvApplyPut(benchmark::State& state) {
    kv::StateMachine sm;
    std::uint64_t seq = 0;
    for (auto _ : state) {
        ++seq;
        sm.apply({.client_id = 1, .seq = seq, .op = kv::Op::Put, .key = "key" + std::to_string(seq % 1000),
                  .value = "value"});
    }
}
BENCHMARK(BM_KvApplyPut);

// The snapshot pause: how long the state machine takes to serialize, by number of keys.
void BM_KvSnapshotTake(benchmark::State& state) {
    const kv::StateMachine sm = state_with(state.range(0));
    std::size_t bytes = 0;
    for (auto _ : state) {
        const std::string s = sm.serialize();
        bytes = s.size();
        benchmark::DoNotOptimize(s.data());
    }
    state.counters["snapshot_bytes"] = static_cast<double>(bytes);
}
BENCHMARK(BM_KvSnapshotTake)->Arg(1'000)->Arg(10'000)->Arg(100'000)->Unit(benchmark::kMillisecond);

void BM_KvSnapshotRestore(benchmark::State& state) {
    const std::string s = state_with(state.range(0)).serialize();
    for (auto _ : state) benchmark::DoNotOptimize(kv::StateMachine::deserialize(s));
}
BENCHMARK(BM_KvSnapshotRestore)->Arg(1'000)->Arg(10'000)->Arg(100'000)->Unit(benchmark::kMillisecond);

// Saving a snapshot durably and compacting the log, by number of keys in the state.
void BM_SaveSnapshotDurably(benchmark::State& state) {
    ScratchDir dir;
    auto storage = *store::FileStorage::open(dir.path);
    const raft::Snapshot snap{.last_included_index = 1, .last_included_term = 1,
                              .data = state_with(state.range(0)).serialize()};
    for (auto _ : state) storage->save_snapshot(snap);
}
BENCHMARK(BM_SaveSnapshotDurably)->Arg(1'000)->Arg(10'000)->Arg(100'000)->Unit(benchmark::kMillisecond)->Iterations(20);

// One log append plus sync: the cost every committed write pays on each server. Arg 1 is
// durable (fsync; F_FULLFSYNC on macOS), 0 skips the sync.
void BM_AppendAndSync(benchmark::State& state) {
    ScratchDir dir;
    auto storage = *store::FileStorage::open(
        dir.path, state.range(0) ? store::FileStorage::Sync::Durable : store::FileStorage::Sync::ProcessCrashOnly);
    raft::Index i = 0;
    for (auto _ : state) {
        const raft::LogEntry e{.term = 1, .index = ++i, .command = std::string(64, 'c')};
        storage->append({&e, 1});
        storage->sync();
    }
}
BENCHMARK(BM_AppendAndSync)->Arg(1)->Arg(0)->Unit(benchmark::kMicrosecond)->Iterations(500);

}  // namespace
