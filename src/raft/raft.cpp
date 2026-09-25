#include "raft/raft.hpp"

#include <algorithm>
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

Raft::Raft(RaftConfig config, Env& env, ApplyFn apply)
    : config_(std::move(config)), env_(env), apply_(std::move(apply)) {
    log_.push_back(LogEntry{.term = 0, .index = 0, .command = {}});
}

void Raft::on_start() {
    PersistentState saved = env_.storage().load();
    current_term_ = saved.current_term;
    voted_for_ = saved.voted_for;
    log_.resize(1);
    log_.insert(log_.end(), saved.log.begin(), saved.log.end());
    role_ = Role::Follower;
    // commit_index and last_applied restart at 0: the leader tells us how far is committed,
    // and the state machine is rebuilt by applying from the start (snapshots come in Phase 8).
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
        if (role_ == Role::Leader) replicate_to_all();
    } else if (tag == kApplyTimer && apply_timer_ == id) {
        apply_timer_.reset();
        apply_committed();
    }
}

ProposeResult Raft::propose(std::string command) {
    if (role_ != Role::Leader) return {.accepted = false, .index = 0, .term = 0, .leader_hint = leader_};
    const Index index = last_log_index() + 1;
    append_durably({LogEntry{.term = current_term_, .index = index, .command = std::move(command)}});
    match_index_[config_.id] = index;
    advance_commit_index();   // a one-node cluster commits at once
    for (NodeId peer : config_.peers) {
        // Peers already being sent entries pick this one up with their next reply.
        if (next_index_[peer] == index) replicate_to(peer);
    }
    return {.accepted = true, .index = index, .term = current_term_, .leader_hint = config_.id};
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
        if (changed) persist_hard_state();   // the vote is durable before the reply leaves
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

// ---- AppendEntries: follower side --------------------------------------------------------

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

    // Consistency check: we must hold prev_log_index with the same term.
    const auto prev_term = term_at(r.prev_log_index);
    if (!prev_term || *prev_term != r.prev_log_term) {
        AppendEntriesReply fail{.term = current_term_, .success = false};
        if (!prev_term) {
            fail.conflict_index = last_log_index() + 1;   // our log is too short
        } else {
            fail.conflict_term = *prev_term;             // skip our whole conflicting term
            Index first = r.prev_log_index;
            while (first > 1 && log_[first - 1].term == *prev_term) --first;
            fail.conflict_index = first;
        }
        send(from, fail);
        return;
    }

    // Append what we lack. An entry that conflicts (same index, different term) is deleted
    // with everything after it; entries we already hold are left alone, so a delayed,
    // shorter AppendEntries can never truncate entries a newer one delivered.
    std::vector<LogEntry> fresh;
    for (const LogEntry& e : r.entries) {
        const auto have = term_at(e.index);
        if (have && *have == e.term) continue;
        if (have) truncate_from(e.index);
        fresh.push_back(e);
    }
    if (!fresh.empty()) append_durably(std::move(fresh));   // durable before we ack

    const Index last_new = r.prev_log_index + r.entries.size();
    if (r.leader_commit > commit_index_) set_commit_index(std::min(r.leader_commit, last_new));
    send(from, AppendEntriesReply{.term = current_term_, .success = true, .match_index = last_new});
}

// ---- AppendEntries: leader side ----------------------------------------------------------

void Raft::handle(NodeId from, const AppendEntriesReply& r) {
    if (r.term > current_term_) {
        step_down(r.term);
        return;
    }
    if (role_ != Role::Leader || r.term != current_term_) return;   // stale

    Index& next = next_index_[from];
    Index& match = match_index_[from];
    if (r.success) {
        match = std::max(match, r.match_index);
        next = std::max(next, match + 1);
        advance_commit_index();
    } else {
        Index retry = r.conflict_index;
        if (r.conflict_term != 0) {
            // If we hold entries from the follower's conflicting term, resume after our last
            // one; otherwise skip the follower's whole term.
            for (Index i = last_log_index(); i > 0 && log_[i].term >= r.conflict_term; --i) {
                if (log_[i].term == r.conflict_term) {
                    retry = i + 1;
                    break;
                }
            }
        }
        // A stale failure must not move next below what the follower already matched.
        next = std::clamp(retry, match + 1, last_log_index() + 1);
    }
    if (next <= last_log_index()) replicate_to(from);   // keep going until it has everything
}

void Raft::replicate_to(NodeId peer) {
    const Index next = next_index_[peer];
    const Index prev = next - 1;
    AppendEntries ae{.term = current_term_, .leader_id = config_.id, .prev_log_index = prev,
                     .prev_log_term = log_[prev].term, .entries = {},
                     .leader_commit = commit_index_};
    const Index end = std::min(last_log_index(), prev + config_.max_entries_per_append);
    ae.entries.assign(log_.begin() + static_cast<std::ptrdiff_t>(next),
                      log_.begin() + static_cast<std::ptrdiff_t>(end) + 1);
    send(peer, ae);
}

void Raft::replicate_to_all() {
    for (NodeId peer : config_.peers) replicate_to(peer);
    cancel_timer(heartbeat_timer_);
    heartbeat_timer_ = env_.after(config_.heartbeat_interval, kHeartbeatTimer);
}

void Raft::advance_commit_index() {
    for (Index n = last_log_index(); n > commit_index_; --n) {
        // §5.4.2: only an entry from the current term is committed by counting replicas.
        // Earlier entries are then committed indirectly, by the Log Matching property.
        if (log_[n].term != current_term_) break;
        std::size_t stored = 0;
        for (const auto& [id, m] : match_index_) stored += m >= n;
        if (stored >= majority()) {
            set_commit_index(n);
            return;
        }
    }
}

void Raft::set_commit_index(Index index) {
    if (index <= commit_index_) return;
    commit_index_ = index;
    // Applying happens in its own event, never inline (implementation guide Phase 3).
    if (!apply_timer_) apply_timer_ = env_.after(Duration{0}, kApplyTimer);
}

void Raft::apply_committed() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        if (apply_) apply_(log_[last_applied_]);
    }
}

// ---- role changes ------------------------------------------------------------------------

void Raft::start_election() {
    ++current_term_;
    role_ = Role::Candidate;
    voted_for_ = config_.id;
    leader_.reset();
    votes_ = {config_.id};
    persist_hard_state();   // term and self-vote are durable before any RequestVote leaves
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
    next_index_.clear();
    match_index_.clear();
    for (NodeId peer : config_.peers) {
        next_index_[peer] = last_log_index() + 1;
        match_index_[peer] = 0;
    }
    env_.trace(std::format("leader term={} last={}/{}", current_term_, last_log_index(),
                           last_log_term()));
    // A no-op from the new term (paper §8): lets entries from earlier terms commit now, via
    // the commit rule, instead of waiting for the next client command.
    append_durably({LogEntry{.term = current_term_, .index = last_log_index() + 1, .command = {}}});
    match_index_[config_.id] = last_log_index();
    advance_commit_index();   // a one-node cluster
    replicate_to_all();       // announce at once, before anyone else times out
}

void Raft::step_down(Term term) {
    const Role was = role_;
    current_term_ = term;
    voted_for_.reset();
    role_ = Role::Follower;
    leader_.reset();
    votes_.clear();
    next_index_.clear();
    match_index_.clear();
    persist_hard_state();
    if (was == Role::Leader) {
        cancel_timer(heartbeat_timer_);
        reset_election_timer();
    }
    if (was != Role::Follower) {
        env_.trace(std::format("step down from {}, term={}", to_string(was), term));
    }
}

// ---- log and storage ---------------------------------------------------------------------

void Raft::append_durably(std::vector<LogEntry> entries) {
    env_.storage().append(entries);
    env_.storage().sync();   // Phase 4's rule, already: durable before anyone is told
    log_.insert(log_.end(), std::make_move_iterator(entries.begin()),
                std::make_move_iterator(entries.end()));
}

void Raft::truncate_from(Index index) {
    // Never reached for a committed entry: Log Matching guarantees the leader agrees on
    // those. The harness checks that this holds.
    env_.trace(std::format("truncate log from {} (was last={})", index, last_log_index()));
    env_.storage().truncate_suffix(index);
    log_.resize(static_cast<std::size_t>(index));
}

// ---- helpers -----------------------------------------------------------------------------

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

void Raft::persist_hard_state() { env_.storage().save_hard_state(current_term_, voted_for_); }

void Raft::send(NodeId to, const Rpc& rpc) { env_.send(encode(to, rpc)); }

const LogEntry* Raft::entry_at(Index i) const {
    if (i == 0 || i > last_log_index()) return nullptr;
    return &log_[i];
}

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
