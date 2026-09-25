#include "kv/server.hpp"

#include <format>

namespace raftkv::kv {

Server::Server(raft::RaftConfig config, raft::Env& env, raft::ApplyFn observer)
    : env_(env),
      observer_(std::move(observer)),
      raft_(std::move(config), env, [this](const raft::LogEntry& e) { on_apply(e); }) {}

void Server::on_start() { raft_.on_start(); }

void Server::on_message(const raft::Message& m) {
    if (m.method == kKvRequest) {
        auto c = parse_request(m);
        if (!c) {
            env_.trace(std::format("dropped request from {}: {}", m.from, c.error()));
            return;
        }
        handle_request(m.from, *c);
        return;
    }
    if (is_kv_message(m)) return;   // a KvReply sent to a server: not for us
    raft_.on_message(m);
    fail_pending_if_deposed();
}

void Server::on_timer(raft::TimerId id, raft::TimerTag tag) {
    raft_.on_timer(id, tag);
    fail_pending_if_deposed();
}

void Server::handle_request(raft::NodeId from, const Command& c) {
    if (raft_.role() != raft::Role::Leader) {
        reply(from, {.client_id = c.client_id, .seq = c.seq, .status = Status::NotLeader,
                     .result = {}, .leader_hint = raft_.leader()});
        return;
    }
    const raft::ProposeResult r = raft_.propose(encode_command(c));
    pending_[r.index] = {.from = from, .client_id = c.client_id, .seq = c.seq, .term = r.term};
}

void Server::on_apply(const raft::LogEntry& e) {
    if (observer_) observer_(e);

    std::optional<Command> applied;
    Result result;
    if (!e.command.empty()) {   // empty: a leader's no-op
        auto c = decode_command(e.command);
        if (c) {
            result = state_.apply(*c);
            applied = *c;
        } else {
            env_.trace(std::format("skipped undecodable entry {}: {}", e.index, c.error()));
        }
    }

    auto it = pending_.find(e.index);
    if (it == pending_.end()) return;
    const Pending p = it->second;
    pending_.erase(it);
    if (applied && applied->client_id == p.client_id && applied->seq == p.seq) {
        reply(p.from, {.client_id = p.client_id, .seq = p.seq, .status = Status::Ok,
                       .result = result, .leader_hint = std::nullopt});
    } else {
        // Another leader's entry took this index: ours was never committed here.
        reply(p.from, {.client_id = p.client_id, .seq = p.seq, .status = Status::NotLeader,
                       .result = {}, .leader_hint = raft_.leader()});
    }
}

void Server::fail_pending_if_deposed() {
    if (pending_.empty()) return;
    const bool still_leading =
        raft_.role() == raft::Role::Leader && raft_.current_term() == pending_.begin()->second.term;
    if (still_leading) return;
    for (const auto& [index, p] : pending_) {
        reply(p.from, {.client_id = p.client_id, .seq = p.seq, .status = Status::NotLeader,
                       .result = {}, .leader_hint = raft_.leader()});
    }
    pending_.clear();
}

void Server::reply(raft::NodeId to, KvReply r) { env_.send(encode_reply(to, r)); }

}  // namespace raftkv::kv
