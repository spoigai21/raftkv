#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

namespace raftkv::test {

// Seeds for a randomized test: RAFTKV_SEED=N replays just that one, otherwise 1..count.
inline std::vector<std::uint64_t> seeds(std::uint64_t count) {
    if (const char* s = std::getenv("RAFTKV_SEED")) return {std::strtoull(s, nullptr, 10)};
    std::vector<std::uint64_t> out;
    for (std::uint64_t i = 1; i <= count; ++i) out.push_back(i);
    return out;
}

}  // namespace raftkv::test
