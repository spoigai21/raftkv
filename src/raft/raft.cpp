#include "raft/raft.hpp"

#include <format>

namespace raftkv::raft {

const char* to_string(Role r) {
    switch (r) {
        case Role::Follower: return "follower";
        case Role::Candidate: return "candidate";
        case Role::Leader: return "leader";
    }
    return "?";
}

Raft::Raft(RaftConfig config, Env& env) : config_(std::move(config)), env_(env) {
    log_.push_back(LogEntry{.term = 0, .index = 0, .command = {}});
}

void Raft::on_start() {
    PersistentState saved = env_.storage().load();
    current_term_ = saved.current_term;
    voted_for_ = saved.voted_for;
    log_.resize(1);
    log_.insert(log_.end(), saved.log.begin(), saved.log.end());
    role_ = Role::Follower;
    env_.trace(std::format("boot term={} voted_for={} last={}/{}", current_term_,
                           voted_for_ ? std::to_string(*voted_for_) : "-", last_log_index(),
                           last_log_term()));
    reset_election_timer();
}

void Raft::on_message(const Message& m) {
    auto rpc = parse(m);
    if (!rpc) {
        env_.trace(std::format("dropped message from {}: {}", m.from, rpc.error()));
        return;
    }
    std::visit([&](const auto& r) { handle(m.from, r); }, *rpc);
}

void Raft::on_timer(TimerId id, TimerTag tag) {
    if (tag == kElectionTimer && election_timer_ == id) {
        election_timer_.reset();
        if (role_ != Role::Leader) start_election();
    } else if (tag == kHeartbeatTimer && heartbeat_timer_ == id) {
        heartbeat_timer_.reset();
        if (role_ == Role::Leader) send_heartbeats();
    }
}

// ---- RequestVote -------------------------------------------------------------------------

void Raft::handle(NodeId from, const RequestVote& r) {
    if (r.candidate_id != from) {
        env_.trace(std::format("dropped RequestVote from {} claiming to be {}", from, r.candidate_id));
        return;
    }
    if (r.term > current_term_) step_down(r.term);

    const bool grant = r.term == current_term_ &&
                       (!voted_for_ || *voted_for_ == from) &&
                       candidate_log_is_up_to_date(r.last_log_index, r.last_log_term);
    if (grant) {
        const bool changed = voted_for_ != from;
        voted_for_ = from;
        if (changed) persist();   // the vote is durable before the reply leaves
        reset_election_timer();   // Figure 2: only when granting, not on every request
    }
    send(from, RequestVoteReply{.term = current_term_, .vote_granted = grant});
}

void Raft::handle(NodeId from, const RequestVoteReply& r) {
    if (r.term > current_term_) {
        step_down(r.term);
        return;
    }
    if (role_ != Role::Candidate || r.term != current_term_ || !r.vote_granted) return;
    votes_.insert(from);
    if (votes_.size() >= majority()) become_leader();
}

// ---- AppendEntries -----------------------------------------------------------------------

void Raft::handle(NodeId from, const AppendEntries& r) {
    if (r.term < current_term_) {
        send(from, AppendEntriesReply{.term = current_term_, .success = false});
        return;
    }
    if (r.term > current_term_) step_down(r.term);

    // Same term from here. A candidate that hears from this term's leader lost the election.
    if (role_ == Role::Candidate) {
        role_ = Role::Follower;
        env_.trace(std::format("follower term={} (lost to {})", current_term_, from));
    }
    if (leader_ != from) {
        leader_ = from;
        env_.trace(std::format("leader is {} term={}", from, current_term_));
    }
    reset_election_timer();

    // Consistency check only; Phase 3 adds appending, truncation and the commit index.
    const auto prev_term = term_at(r.prev_log_index);
    const bool ok = prev_term && *prev_term == r.prev_log_term;
    send(from, AppendEntriesReply{.term = current_term_, .success = ok});
}

void Raft::handle(NodeId, const AppendEntriesReply& r) {
    if (r.term > current_term_) step_down(r.term);
    // Phase 3: next_index / match_index bookkeeping.
}

// ---- role changes ------------------------------------------------------------------------

void Raft::start_election() {
    ++current_term_;
    role_ = Role::Candidate;
    voted_for_ = config_.id;
    leader_.reset();
    votes_ = {config_.id};
    persist();   // term and self-vote are durable before any RequestVote leaves
    env_.trace(std::format("candidate term={}", current_term_));
    reset_election_timer();   // if this election splits, try again with a fresh timeout

    if (votes_.size() >= majority()) {   // a one-node cluster
        become_leader();
        return;
    }
    const RequestVote rv{.term = current_term_, .candidate_id = config_.id,
                         .last_log_index = last_log_index(), .last_log_term = last_log_term()};
    for (NodeId peer : config_.peers) send(peer, rv);
}

void Raft::become_leader() {
    role_ = Role::Leader;
    leader_ = config_.id;
    votes_.clear();
    cancel_timer(election_timer_);
    env_.trace(std::format("leader term={}", current_term_));
    send_heartbeats();   // announce at once, before anyone else times out
}

void Raft::step_down(Term term) {
    const Role was = role_;
    current_term_ = term;
    voted_for_.reset();
    role_ = Role::Follower;
    leader_.reset();
    votes_.clear();
    persist();
    if (was == Role::Leader) {
        cancel_timer(heartbeat_timer_);
        reset_election_timer();
    }
    if (was != Role::Follower) {
        env_.trace(std::format("step down from {}, term={}", to_string(was), term));
    }
}

// ---- helpers -----------------------------------------------------------------------------

void Raft::send_heartbeats() {
    const AppendEntries hb{.term = current_term_, .leader_id = config_.id,
                           .prev_log_index = last_log_index(), .prev_log_term = last_log_term(),
                           .entries = {}, .leader_commit = 0};
    for (NodeId peer : config_.peers) send(peer, hb);
    cancel_timer(heartbeat_timer_);
    heartbeat_timer_ = env_.after(config_.heartbeat_interval, kHeartbeatTimer);
}

void Raft::reset_election_timer() {
    cancel_timer(election_timer_);
    const auto lo = static_cast<std::uint64_t>(config_.election_timeout_min.count());
    const auto hi = static_cast<std::uint64_t>(config_.election_timeout_max.count());
    // Randomized on every reset, mapped by hand rather than with a std:: distribution (§3.2).
    const std::uint64_t us = hi > lo ? lo + env_.random() % (hi - lo + 1) : lo;
    election_timer_ = env_.after(Duration(static_cast<Duration::rep>(us)), kElectionTimer);
}

void Raft::cancel_timer(std::optional<TimerId>& timer) {
    if (timer) env_.cancel(*timer);
    timer.reset();
}

void Raft::persist() { env_.storage().save_hard_state(current_term_, voted_for_); }

void Raft::send(NodeId to, const Rpc& rpc) { env_.send(encode(to, rpc)); }

std::optional<Term> Raft::term_at(Index i) const {
    if (i > last_log_index()) return std::nullopt;
    return log_[i].term;   // Phase 8 (compaction) changes this, and only this, lookup
}

// §5.4.1: the candidate's log must be at least as up to date as ours, comparing the last
// entry's term first and then the index.
bool Raft::candidate_log_is_up_to_date(Index last_index, Term last_term) const {
    if (last_term != last_log_term()) return last_term > last_log_term();
    return last_index >= last_log_index();
}

}  // namespace raftkv::raft
