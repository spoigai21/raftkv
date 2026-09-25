#pragma once

#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
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
    std::size_t max_entries_per_append = 64;
};

enum class Role { Follower, Candidate, Leader };

// Receives committed entries, in log order, each exactly once per boot. Entries with an empty
// command are the no-ops a new leader appends; a state machine ignores them.
using ApplyFn = std::function<void(const LogEntry&)>;

struct ProposeResult {
    bool accepted = false;
    Index index = 0;   // where the command will be, if it commits
    Term term = 0;
    std::optional<NodeId> leader_hint;   // when not accepted: who to try instead, if known
};

// One Raft server, implemented from Figure 2 of the paper. Single-threaded and event-driven
// (implementation guide §3.1): everything happens in on_start/on_message/on_timer/propose,
// and the outside world is reached only through Env.
class Raft final : public Node {
public:
    Raft(RaftConfig config, Env& env, ApplyFn apply = {});

    void on_start() override;
    void on_message(const Message& m) override;
    void on_timer(TimerId id, TimerTag tag) override;

    // Appends a command to the log if this node is the leader. Accepted is not committed:
    // the entry can still be lost if leadership changes before a majority stores it.
    ProposeResult propose(std::string command);

    NodeId id() const { return config_.id; }
    Role role() const { return role_; }
    Term current_term() const { return current_term_; }
    std::optional<NodeId> voted_for() const { return voted_for_; }
    std::optional<NodeId> leader() const { return leader_; }   // as far as this node knows
    Index commit_index() const { return commit_index_; }
    Index last_applied() const { return last_applied_; }

    Index last_log_index() const { return log_.back().index; }
    Term last_log_term() const { return log_.back().term; }
    // nullptr if this node does not hold entry `i` (index 0 is the sentinel).
    const LogEntry* entry_at(Index i) const;

private:
    enum : TimerTag { kElectionTimer = 1, kHeartbeatTimer = 2, kApplyTimer = 3 };

    void handle(NodeId from, const RequestVote& r);
    void handle(NodeId from, const RequestVoteReply& r);
    void handle(NodeId from, const AppendEntries& r);
    void handle(NodeId from, const AppendEntriesReply& r);

    void start_election();
    void become_leader();
    // Adopts a newer term seen in any RPC: back to follower, vote cleared, persisted.
    void step_down(Term term);

    // Leader: sends `peer` everything from its next_index (or a heartbeat if it has it all).
    void replicate_to(NodeId peer);
    void replicate_to_all();
    // Leader: commits the newest entry from the current term that a majority stores (§5.4.2).
    void advance_commit_index();
    void set_commit_index(Index index);
    void apply_committed();

    // Appends to the in-memory log and to storage, and syncs before returning.
    void append_durably(std::vector<LogEntry> entries);
    void truncate_from(Index index);

    void reset_election_timer();
    void cancel_timer(std::optional<TimerId>& timer);
    void persist_hard_state();
    void send(NodeId to, const Rpc& rpc);

    std::size_t majority() const { return (config_.peers.size() + 1) / 2 + 1; }
    std::optional<Term> term_at(Index i) const;
    bool candidate_log_is_up_to_date(Index last_index, Term last_term) const;

    RaftConfig config_;
    Env& env_;
    ApplyFn apply_;

    // Persistent state (Figure 2): saved through Storage before any reply that depends on it.
    Term current_term_ = 0;
    std::optional<NodeId> voted_for_;
    std::vector<LogEntry> log_;   // log_[0] is the term-0 sentinel (implementation guide §3.3)

    // Volatile state.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_;
    Index commit_index_ = 0;
    Index last_applied_ = 0;
    std::set<NodeId> votes_;                  // candidate only
    std::map<NodeId, Index> next_index_;      // leader only
    std::map<NodeId, Index> match_index_;     // leader only
    std::optional<TimerId> election_timer_;
    std::optional<TimerId> heartbeat_timer_;
    std::optional<TimerId> apply_timer_;
};

const char* to_string(Role r);

}  // namespace raftkv::raft
