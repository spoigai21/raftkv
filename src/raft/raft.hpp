#pragma once

#include <optional>
#include <set>
#include <vector>

#include "raft/env.hpp"
#include "raft/messages.hpp"

namespace raftkv::raft {

struct RaftConfig {
    NodeId id = 0;
    std::vector<NodeId> peers;   // every other member of the cluster
    Duration election_timeout_min{150'000};   // implementation guide §3.5
    Duration election_timeout_max{300'000};
    Duration heartbeat_interval{50'000};
};

enum class Role { Follower, Candidate, Leader };

// One Raft server, implemented from Figure 2 of the paper. Single-threaded and event-driven
// (implementation guide §3.1): everything happens in on_start/on_message/on_timer, and the
// outside world is reached only through Env.
//
// Phase 2 covers leader election. The log is kept and compared in votes, but nothing
// replicates it yet.
class Raft final : public Node {
public:
    Raft(RaftConfig config, Env& env);

    void on_start() override;
    void on_message(const Message& m) override;
    void on_timer(TimerId id, TimerTag tag) override;

    NodeId id() const { return config_.id; }
    Role role() const { return role_; }
    Term current_term() const { return current_term_; }
    std::optional<NodeId> voted_for() const { return voted_for_; }
    std::optional<NodeId> leader() const { return leader_; }   // as far as this node knows

    Index last_log_index() const { return log_.back().index; }
    Term last_log_term() const { return log_.back().term; }

private:
    enum : TimerTag { kElectionTimer = 1, kHeartbeatTimer = 2 };

    void handle(NodeId from, const RequestVote& r);
    void handle(NodeId from, const RequestVoteReply& r);
    void handle(NodeId from, const AppendEntries& r);
    void handle(NodeId from, const AppendEntriesReply& r);

    void start_election();
    void become_leader();
    // Adopts a newer term seen in any RPC: back to follower, vote cleared, persisted.
    void step_down(Term term);

    void send_heartbeats();
    void reset_election_timer();
    void cancel_timer(std::optional<TimerId>& timer);
    void persist();
    void send(NodeId to, const Rpc& rpc);

    std::size_t majority() const { return (config_.peers.size() + 1) / 2 + 1; }
    std::optional<Term> term_at(Index i) const;
    bool candidate_log_is_up_to_date(Index last_index, Term last_term) const;

    RaftConfig config_;
    Env& env_;

    // Persistent state (Figure 2): saved through Storage before any reply that depends on it.
    Term current_term_ = 0;
    std::optional<NodeId> voted_for_;
    std::vector<LogEntry> log_;   // log_[0] is the term-0 sentinel (implementation guide §3.3)

    // Volatile state.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_;
    std::set<NodeId> votes_;   // candidate only
    std::optional<TimerId> election_timer_;
    std::optional<TimerId> heartbeat_timer_;
};

const char* to_string(Role r);

}  // namespace raftkv::raft
