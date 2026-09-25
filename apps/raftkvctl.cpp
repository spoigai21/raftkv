// raftkvctl: command-line client.
#include <cstdio>

#include "version.hpp"

int main() {
    std::printf("raftkvctl %.*s: not implemented yet (Phase 5)\n",
                static_cast<int>(raftkv::version().size()), raftkv::version().data());
    return 1;
}
