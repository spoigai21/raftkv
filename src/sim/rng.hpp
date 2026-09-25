#pragma once

#include <cstdint>

namespace raftkv::sim {

// SplitMix64. Every mapping from the raw stream to a range is done here, by hand: the
// standard library's distributions differ between libc++ and libstdc++, which would make
// a seed replay differently on macOS and Linux (implementation guide §3.2).
class Rng {
public:
    explicit Rng(std::uint64_t seed) : state_(seed) {}

    std::uint64_t next() {
        std::uint64_t z = (state_ += 0x9e3779b97f4a7c15ULL);
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
        return z ^ (z >> 31);
    }

    // Uniform in [lo, hi]. The modulo bias is below 2^-40 for the ranges used here.
    std::uint64_t between(std::uint64_t lo, std::uint64_t hi) {
        if (hi <= lo) return lo;
        const std::uint64_t span = hi - lo + 1;
        return span == 0 ? next() : lo + next() % span;   // span == 0: the full 64-bit range
    }

    // True with probability p. Uses the top 53 bits, so the comparison is exact everywhere.
    bool chance(double p) {
        if (p <= 0.0) return false;
        if (p >= 1.0) return true;
        return static_cast<double>(next() >> 11) * 0x1.0p-53 < p;
    }

private:
    std::uint64_t state_;
};

}  // namespace raftkv::sim
