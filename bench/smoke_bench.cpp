// Phase 0 smoke benchmark: proves Google Benchmark builds and runs in the release job.
#include <benchmark/benchmark.h>

#include "version.hpp"

static void BM_Version(benchmark::State& state) {
    for (auto _ : state) benchmark::DoNotOptimize(raftkv::version());
}
BENCHMARK(BM_Version);
