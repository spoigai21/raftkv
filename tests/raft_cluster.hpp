#pragma once

// Test harness: a simulated cluster of Raft nodes whose safety invariants (Figure 3 of the
// paper) are checked after every single event, plus a seeded fault script and a client
// workload for chaos runs.

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <map>
#include <memory>
#include <optional>
#include <set>
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
    RaftCluster(std::uint64_t seed, int n)
        : ids_(make_ids(n)),
          sim_(seed, ids_, [this](raft::NodeId me, raft::Env& env) { return make_node(me, env); }) {
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

    // Proposes to the current leader, if there is one.
    std::optional<raft::ProposeResult> propose(std::string command) {
        auto id = leader();
        if (!id) return std::nullopt;
        return raft(*id)->propose(std::move(command));
    }

    // Commands applied by node `id` since it last booted, in order, without leader no-ops.
    std::vector<std::string> applied(raft::NodeId id) const {
        std::vector<std::string> out;
        for (const auto& e : applied_.at(id)) if (!e.command.empty()) out.push_back(e.command);
        return out;
    }

    // Highest index any node has ever seen committed.
    raft::Index max_committed() const { return committed_.empty() ? 0 : committed_.rbegin()->first; }

    // Every node is up and holds the same log, all of it committed and applied.
    bool converged() {
        const raft::Raft* first = nullptr;
        for (raft::NodeId id : ids_) {
            const raft::Raft* r = live(id);
            if (r == nullptr || r->commit_index() != r->last_log_index() ||
                r->last_applied() != r->commit_index()) {
                return false;
            }
            if (first == nullptr) {
                first = r;
                continue;
            }
            if (r->last_log_index() != first->last_log_index()) return false;
            for (raft::Index i = 1; i <= r->last_log_index(); ++i) {
                if (!(*r->entry_at(i) == *first->entry_at(i))) return false;
            }
        }
        return true;
    }

    bool wait_for_convergence(raft::Duration timeout) {
        return sim_.run_until([this] { return converged(); }, sim_.now() + timeout);
    }

    // A client that proposes "<prefix><n>" to whoever leads, every `every`, until `until`.
    void schedule_workload(raft::Duration every, raft::Time until, std::string prefix = "c") {
        auto n = std::make_shared<int>(0);
        for (raft::Time t = sim_.now() + every; t < until; t = t + every) {
            sim_.schedule(t, "propose", [this, n, prefix] {
                if (propose(prefix + std::to_string(*n))) ++*n;
            });
        }
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

    // Called on every boot. The state machine (here, a list of applied entries) is volatile,
    // so it starts empty and Raft replays the log into it.
    std::unique_ptr<raft::Node> make_node(raft::NodeId me, raft::Env& env) {
        raft::RaftConfig cfg{.id = me, .peers = {}};
        for (raft::NodeId p : ids_) if (p != me) cfg.peers.push_back(p);
        applied_[me].clear();
        commit_checked_[me] = 0;
        return std::make_unique<raft::Raft>(cfg, env, [this, me](const raft::LogEntry& e) {
            on_apply(me, e);
        });
    }

    void on_apply(raft::NodeId id, const raft::LogEntry& e) {
        auto& mine = applied_[id];
        if (e.index != mine.size() + 1) {
            violation(std::format("node {} applied index {} after {}", id, e.index, mine.size()));
        }
        mine.push_back(e);
        // State Machine Safety: nobody ever applies a different entry at the same index.
        auto [it, fresh] = applied_at_.emplace(e.index, e);
        if (!fresh && !(it->second == e)) {
            violation(std::format("index {} applied as {}/'{}' and {}/'{}'", e.index, it->second.term,
                                  it->second.command, e.term, e.command));
        }
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

            // Committed entries never change: record each index once any node commits it, and
            // every later commit of that index must be the same entry.
            for (raft::Index i = commit_checked_[id] + 1; i <= r->commit_index(); ++i) {
                const raft::LogEntry* e = r->entry_at(i);
                if (e == nullptr) {
                    violation(std::format("node {} committed {} past its log end", id, i));
                    break;
                }
                auto [it, fresh] = committed_.emplace(i, *e);
                if (!fresh && !(it->second == *e)) {
                    violation(std::format("index {} committed as term {} and term {}", i,
                                          it->second.term, e->term));
                }
            }
            commit_checked_[id] = std::max(commit_checked_[id], r->commit_index());

            // Leader Completeness: a leader holds every entry committed before its term.
            if (r->role() == raft::Role::Leader && checked_leaders_.emplace(id, term).second) {
                for (const auto& [i, e] : committed_) {
                    const raft::LogEntry* mine = r->entry_at(i);
                    if (mine == nullptr || !(*mine == e)) {
                        violation(std::format("leader {} of term {} lacks committed index {}", id,
                                              term, i));
                        break;
                    }
                }
            }

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
    std::map<raft::Index, raft::LogEntry> committed_;
    std::map<raft::NodeId, raft::Index> commit_checked_;
    std::set<std::pair<raft::NodeId, raft::Term>> checked_leaders_;
    std::map<raft::NodeId, std::vector<raft::LogEntry>> applied_;
    std::map<raft::Index, raft::LogEntry> applied_at_;
    std::vector<std::string> violations_;
};

}  // namespace raftkv::test
