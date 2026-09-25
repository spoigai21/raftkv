// raftkvd: one server process.
// Planned usage: raftkvd --id 1 --cluster cluster.json --data-dir data/1
#include <cstdio>

#include "version.hpp"

int main() {
    std::printf("raftkvd %.*s: not implemented yet (Phase 5)\n",
                static_cast<int>(raftkv::version().size()), raftkv::version().data());
    return 1;
}
