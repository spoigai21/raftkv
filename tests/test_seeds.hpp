#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <vector>

// Sanitizer builds (the dev and tsan presets) set this to run a fraction of the seeds in the
// heavy chaos tests. The rel preset runs all of them. See CMakePresets.json.
#ifndef RAFTKV_SEED_DIVISOR
#define RAFTKV_SEED_DIVISOR 1
#endif

namespace raftkv::test {

// Seeds for a randomized test: RAFTKV_SEED=N replays just that one, otherwise 1..count.
inline std::vector<std::uint64_t> seeds(std::uint64_t count) {
    if (const char* s = std::getenv("RAFTKV_SEED")) return {std::strtoull(s, nullptr, 10)};
    std::vector<std::uint64_t> out;
    for (std::uint64_t i = 1; i <= count; ++i) out.push_back(i);
    return out;
}

// For the heavy chaos tests: `count` seeds in release builds, a fraction under sanitizers,
// whose job is memory and thread errors rather than seed coverage.
inline std::uint64_t chaos_seed_count(std::uint64_t count) {
    return std::max<std::uint64_t>(1, count / RAFTKV_SEED_DIVISOR);
}
inline std::vector<std::uint64_t> chaos_seeds(std::uint64_t count) { return seeds(chaos_seed_count(count)); }

}  // namespace raftkv::test
