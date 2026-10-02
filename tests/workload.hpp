#pragma once

// A contended client workload for KV clusters, recorded as a History for the
// linearizability checker (implementation guide Phase 6). Shared by the linearizability
// tests and the fault matrix.

#include <algorithm>
#include <array>
#include <format>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "history.hpp"
#include "raft_cluster.hpp"

namespace raftkv::test {

inline constexpr std::array<const char*, 3> kWorkloadKeys{"x", "y", "z"};

// The workload's state. `next` issues a client's next operation; callbacks refer to it
// through a weak_ptr so that it does not own itself (a leak LeakSanitizer would report).
struct Workload {
    std::shared_ptr<History> history = std::make_shared<History>();
    std::shared_ptr<std::function<void(raft::NodeId)>> next =
        std::make_shared<std::function<void(raft::NodeId)>>();
};

// Starts the workload: each client runs get/put/append operations (40/30/30) on three keys,
// back to back, until `stop_issuing`. Values are unique so the checker can tell every write
// apart. The history fills in as the simulation runs; keep the Workload alive for the run.
inline Workload run_workload(RaftCluster& c, std::uint64_t seed, raft::Time stop_issuing) {
    Workload w;
    auto history = w.history;
    auto rng = std::make_shared<sim::Rng>(seed * 7919 + 17);
    auto counter = std::make_shared<std::map<raft::NodeId, int>>();
    std::weak_ptr<std::function<void(raft::NodeId)>> next = w.next;
    *w.next = [&c, history, rng, counter, next, stop_issuing](raft::NodeId id) {
        if (c.sim().now() >= stop_issuing) return;
        const int n = (*counter)[id]++;
        const std::string key = kWorkloadKeys[rng->between(0, kWorkloadKeys.size() - 1)];
        const auto roll = rng->between(0, 9);
        const kv::Op op = roll < 4 ? kv::Op::Get : roll < 7 ? kv::Op::Put : kv::Op::Append;
        const std::string value = op == kv::Op::Get ? "" : std::format("{}.{};", id, n);
        const std::size_t i = history->invoke(id, op, key, value, c.sim().now());
        auto done = [&c, history, next, i, id](const kv::Result& r) {
            history->complete(i, r, c.sim().now());
            if (auto issue = next.lock()) (*issue)(id);
        };
        auto& client = c.client(id);
        if (op == kv::Op::Get) client.get(key, done);
        else if (op == kv::Op::Put) client.put(key, value, done);
        else client.append(key, value, done);
    };
    for (raft::NodeId id : c.client_ids()) (*w.next)(id);
    return w;
}

// Runs until no client has a request outstanding, or `deadline`.
inline bool wait_for_clients_idle(RaftCluster& c, raft::Time deadline) {
    return c.sim().run_until([&] {
        return std::ranges::none_of(c.client_ids(), [&](raft::NodeId id) { return c.client(id).busy(); });
    }, deadline);
}

}  // namespace raftkv::test
