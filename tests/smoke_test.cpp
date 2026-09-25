// Phase 0 smoke test: proves gtest, the sanitizer runtimes and vcpkg linking all work.
#include <gtest/gtest.h>

#include <limits>
#include <type_traits>

#include "raft/types.hpp"
#include "version.hpp"

TEST(Smoke, VersionIsSet) { EXPECT_FALSE(raftkv::version().empty()); }

TEST(Smoke, RaftTypesAreUnsigned64) {
    using namespace raftkv::raft;
    static_assert(std::is_unsigned_v<Term> && sizeof(Term) == 8);
    static_assert(std::is_unsigned_v<Index> && sizeof(Index) == 8);
    EXPECT_EQ(std::numeric_limits<Index>::min(), Index{0});
}
