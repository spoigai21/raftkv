#pragma once

// Test harness: a simulated cluster of Raft nodes whose safety invariants are checked after
// every single event, plus a seeded fault script for chaos runs.

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "raft/messages.hpp"
#include "raft/raft.hpp"
#include "sim/sim.hpp"

namespace raftkv::test {

using namespace std::chrono_literals;

class RaftCluster {
public:
    RaftCluster(std::uint64_t seed, int n) : ids_(make_ids(n)), sim_(seed, ids_, factory(ids_)) {
        sim_.describe_messages_with(raft::describe);
        sim_.after_each_event([this] { check_invariants(); });
        sim_.faults().delay_min = 1ms;
        sim_.faults().delay_max = 10ms;
        sim_.faults().reorder = true;
    }

    RaftCluster(const RaftCluster&) = delete;
    RaftCluster& operator=(const RaftCluster&) = delete;

    // RAFTKV_DUMP_LOG=1 prints the whole event log when the cluster goes away. Pair it with
    // RAFTKV_SEED=N to read exactly what happened in one run.
    ~RaftCluster() {
        if (std::getenv("RAFTKV_DUMP_LOG") == nullptr) return;
        std::fprintf(stderr, "=== event log, seed %llu ===\n",
                     static_cast<unsigned long long>(sim_.seed()));
        for (const std::string& line : sim_.event_log()) std::fprintf(stderr, "%s\n", line.c_str());
    }

    sim::Sim& sim() { return sim_; }
    const std::vector<raft::NodeId>& ids() const { return ids_; }
    raft::Raft* raft(raft::NodeId id) { return dynamic_cast<raft::Raft*>(sim_.node(id)); }

    // The leader of the newest term: up, not paused, and no live node has seen a later term.
    std::optional<raft::NodeId> leader() {
        raft::Term newest = 0;
        for (raft::NodeId id : ids_) {
            if (auto* r = live(id)) newest = std::max(newest, r->current_term());
        }
        for (raft::NodeId id : ids_) {
            auto* r = live(id);
            if (r && r->role() == raft::Role::Leader && r->current_term() == newest) return id;
        }
        return std::nullopt;
    }

    bool wait_for_leader(raft::Duration timeout) {
        return sim_.run_until([this] { return leader().has_value(); }, sim_.now() + timeout);
    }

    // Every live node agrees on who leads, in the same term.
    bool all_follow(raft::NodeId leader_id) {
        const raft::Term term = raft(leader_id)->current_term();
        for (raft::NodeId id : ids_) {
            auto* r = live(id);
            if (r && (r->leader() != leader_id || r->current_term() != term)) return false;
        }
        return true;
    }

    const std::vector<std::string>& violations() const { return violations_; }

    // For failure messages: how to replay, and what happened last.
    std::string context(std::size_t tail = 40) const {
        return std::format("replay with RAFTKV_SEED={}\n--- last events ---\n{}", sim_.seed(),
                           sim_.log_tail(tail));
    }

    // Random crashes, restarts, partitions, pauses and loss for `length`, drawn from `seed`;
    // then everything heals at once so liveness can be checked.
    void schedule_chaos(std::uint64_t seed, raft::Duration length) {
        auto rng = std::make_shared<sim::Rng>(seed * 0x9e3779b97f4a7c15ULL + 1);
        auto pick = [this, rng](auto&& pred) -> std::optional<raft::NodeId> {
            std::vector<raft::NodeId> ok;
            for (raft::NodeId id : ids_) if (pred(id)) ok.push_back(id);
            if (ok.empty()) return std::nullopt;
            return ok[rng->between(0, ok.size() - 1)];
        };
        raft::Time t{};
        const raft::Time end = raft::Time{} + length;
        while (true) {
            t = t + raft::Duration(static_cast<raft::Duration::rep>(rng->between(100'000, 600'000)));
            if (t >= end) break;
            sim_.schedule(t, "chaos", [this, rng, pick] {
                switch (rng->between(0, 5)) {
                    case 0:
                        if (auto id = pick([&](raft::NodeId i) { return sim_.is_up(i); })) {
                            sim_.crash(*id);
                        }
                        break;
                    case 1:
                        if (auto id = pick([&](raft::NodeId i) { return !sim_.is_up(i); })) {
                            sim_.restart(*id);
                        }
                        break;
                    case 2: {
                        std::vector<raft::NodeId> a, b;
                        for (raft::NodeId id : ids_) (rng->chance(0.5) ? a : b).push_back(id);
                        sim_.faults().partitions = {a, b};
                        break;
                    }
                    case 3: sim_.faults().partitions.clear(); break;
                    case 4:
                        if (auto id = pick([&](raft::NodeId i) { return sim_.is_paused(i); })) {
                            sim_.resume(*id);
                        } else if (auto up = pick([&](raft::NodeId i) { return sim_.is_up(i); })) {
                            sim_.pause(*up);
                        }
                        break;
                    default:
                        sim_.faults().drop_rate = static_cast<double>(rng->between(0, 20)) / 100.0;
                        break;
                }
            });
        }
        sim_.schedule(end, "heal everything", [this] {
            sim_.faults().partitions.clear();
            sim_.faults().drop_rate = 0.0;
            for (raft::NodeId id : ids_) {
                if (!sim_.is_up(id)) sim_.restart(id);
                if (sim_.is_paused(id)) sim_.resume(id);
            }
        });
    }

private:
    static std::vector<raft::NodeId> make_ids(int n) {
        std::vector<raft::NodeId> ids;
        for (int i = 1; i <= n; ++i) ids.push_back(static_cast<raft::NodeId>(i));
        return ids;
    }

    static sim::Sim::NodeFactory factory(std::vector<raft::NodeId> ids) {
        return [ids](raft::NodeId me, raft::Env& env) {
            raft::RaftConfig cfg{.id = me, .peers = {}};
            for (raft::NodeId p : ids) if (p != me) cfg.peers.push_back(p);
            return std::make_unique<raft::Raft>(cfg, env);
        };
    }

    raft::Raft* live(raft::NodeId id) {
        return sim_.is_up(id) && !sim_.is_paused(id) ? raft(id) : nullptr;
    }

    void check_invariants() {
        for (raft::NodeId id : ids_) {
            const raft::Raft* r = raft(id);
            if (r == nullptr) continue;
            const raft::Term term = r->current_term();

            // Election Safety (Figure 3): at most one leader per term, ever.
            if (r->role() == raft::Role::Leader) {
                auto [it, fresh] = leader_of_term_.emplace(term, id);
                if (!fresh && it->second != id) {
                    violation(std::format("two leaders in term {}: {} and {}", term, it->second, id));
                }
            }
            // A node's term never goes backwards, even across a crash.
            raft::Term& seen = highest_term_[id];
            if (term < seen) violation(std::format("node {} term went back {} -> {}", id, seen, term));
            seen = std::max(seen, term);

            // A node votes at most once per term, even across a crash.
            if (auto v = r->voted_for()) {
                auto [it, fresh] = vote_in_term_.emplace(std::pair(id, term), *v);
                if (!fresh && it->second != *v) {
                    violation(std::format("node {} voted for {} and {} in term {}", id,
                                          it->second, *v, term));
                }
            }
        }
    }

    void violation(std::string what) {
        if (violations_.size() < 10) {
            violations_.push_back(std::format("t={}us {}", sim_.now().since_start.count(), what));
        }
    }

    std::vector<raft::NodeId> ids_;
    sim::Sim sim_;
    std::map<raft::Term, raft::NodeId> leader_of_term_;
    std::map<raft::NodeId, raft::Term> highest_term_;
    std::map<std::pair<raft::NodeId, raft::Term>, raft::NodeId> vote_in_term_;
    std::vector<std::string> violations_;
};

}  // namespace raftkv::test
