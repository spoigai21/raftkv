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
    // Group commit: proposals are appended without syncing, and one sync, in an event of its
    // own, covers every proposal that arrived meanwhile. Off: each proposal syncs on its own.
    bool group_commit = true;
    // PreVote (Ongaro's thesis §9.6): before raising its term, a node asks whether it could
    // win. Nodes that heard from a leader within election_timeout_min say no, so a node coming
    // back from a partition, or a slow one, cannot force an election.
    bool pre_vote = true;
    // CheckQuorum: a leader that has not heard from a majority within election_timeout_max
    // steps down, so a leader cut off from the others stops taking requests.
    bool check_quorum = true;
    // Take a snapshot once this many applied entries have built up since the last one
    // (implementation guide Phase 8). 0: never, and the log grows without bound.
    Index snapshot_every = 0;
};

enum class Role { Follower, Candidate, Leader };

// Receives committed entries, in log order, each exactly once per boot. Entries with an empty
// command are the no-ops a new leader appends; a state machine ignores them.
using ApplyFn = std::function<void(const LogEntry&)>;

// How Raft gets at the state machine for snapshots. `take` serializes it as of the last
// applied entry; `restore` replaces it with a snapshot's contents (on boot, and when the
// leader sends one). Without them, Raft never snapshots.
struct SnapshotHooks {
    std::function<std::string()> take;
    std::function<void(const std::string&)> restore;
};

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
    Raft(RaftConfig config, Env& env, ApplyFn apply = {}, SnapshotHooks snapshots = {});

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
    // nullptr if this node does not hold entry `i`: past its end, or compacted into the
    // snapshot (index 0, before any snapshot, is the sentinel).
    const LogEntry* entry_at(Index i) const;
    // Entries up to and including this index live only in the snapshot (0: no snapshot).
    Index snapshot_index() const { return log_.front().index; }

private:
    enum : TimerTag { kElectionTimer = 1, kHeartbeatTimer = 2, kApplyTimer = 3, kFlushTimer = 4 };

    void handle(NodeId from, const RequestVote& r);
    void handle(NodeId from, const RequestVoteReply& r);
    void handle(NodeId from, const AppendEntries& r);
    void handle(NodeId from, const AppendEntriesReply& r);
    void handle(NodeId from, const InstallSnapshot& r);
    void handle(NodeId from, const InstallSnapshotReply& r);

    void start_pre_vote();
    void start_election();
    // A leader that lost touch with the majority becomes a follower, in the same term.
    void lose_leadership();
    void become_leader();
    // Adopts a newer term seen in any RPC: back to follower, vote cleared, persisted.
    void step_down(Term term);

    // Leader: sends `peer` everything from its next_index (or a heartbeat if it has it all).
    void replicate_to(NodeId peer);
    void replicate_to_all();
    // Leader: commits the newest entry from the current term that a majority stores (§5.4.2).
    void advance_commit_index();
    void set_commit_index(Index index);
    // Group commit: sync what propose() appended, then count it and send it on.
    void flush_proposals();
    void apply_committed();
    void maybe_take_snapshot();
    // Replaces log entries up to s.last_included_index with the snapshot, in memory.
    void compact_to(const Snapshot& s);

    // Appends to the in-memory log and to storage, and syncs before returning.
    void append_durably(std::vector<LogEntry> entries);
    void truncate_from(Index index);

    void reset_election_timer();
    void cancel_timer(std::optional<TimerId>& timer);
    void persist_hard_state();
    void send(NodeId to, const Rpc& rpc);

    std::size_t majority() const { return (config_.peers.size() + 1) / 2 + 1; }
    std::optional<Term> term_at(Index i) const;
    // Entry `i`, which must be in the log or be the sentinel. The one place that turns a log
    // index into a position in log_ (implementation guide §3.3).
    const LogEntry& at(Index i) const;
    bool candidate_log_is_up_to_date(Index last_index, Term last_term) const;

    RaftConfig config_;
    Env& env_;
    ApplyFn apply_;
    SnapshotHooks snapshots_;

    // Persistent state (Figure 2): saved through Storage before any reply that depends on it.
    Term current_term_ = 0;
    std::optional<NodeId> voted_for_;
    // log_[0] is a sentinel: index 0, term 0 before any snapshot, and the snapshot's last
    // included entry after one (implementation guide §3.3).
    std::vector<LogEntry> log_;
    std::optional<Snapshot> snapshot_;   // the latest snapshot, kept to send to followers

    // Volatile state.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_;
    Index commit_index_ = 0;
    Index last_applied_ = 0;
    std::set<NodeId> votes_;                  // candidate only
    bool pre_voting_ = false;                 // asking for pre-votes for current_term_ + 1
    std::set<NodeId> pre_votes_;
    std::optional<Time> leader_contact_;      // when we last heard from a valid leader
    std::map<NodeId, Time> last_ack_;         // leader only: last reply from each peer
    Time leader_since_{};
    std::map<NodeId, Index> next_index_;      // leader only
    std::map<NodeId, Index> match_index_;     // leader only
    // Leader only: the last index sent to each peer. Above match_index_, an AppendEntries is
    // outstanding, and its reply will carry on from there.
    std::map<NodeId, Index> sent_index_;
    std::optional<TimerId> election_timer_;
    std::optional<TimerId> heartbeat_timer_;
    std::optional<TimerId> apply_timer_;
    std::optional<TimerId> flush_timer_;
    Index unflushed_from_ = 0;   // first proposal appended since the last flush
};

const char* to_string(Role r);

}  // namespace raftkv::raft
