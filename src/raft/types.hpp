#pragma once

#include <cstdint>

// Index and ID conventions fixed in raftkv-implementation.md §3.3.
namespace raftkv::raft {

using Term = std::uint64_t;
using Index = std::uint64_t;   // the log is 1-indexed; index 0 is a term-0 sentinel
using NodeId = std::uint32_t;

}  // namespace raftkv::raft
