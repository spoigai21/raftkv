#include "kv/client.hpp"

#include <algorithm>
#include <format>
#include <stdexcept>

namespace raftkv::kv {

Client::Client(ClientConfig config, raft::Env& env)
    : config_(std::move(config)), env_(env), client_id_(env.random() | 1) {
    if (config_.servers.empty()) throw std::invalid_argument("kv::Client needs at least one server");
    target_ = static_cast<std::size_t>(env_.random() % config_.servers.size());
}

void Client::get(std::string key, Callback done) { start(Op::Get, std::move(key), {}, std::move(done)); }

void Client::put(std::string key, std::string value, Callback done) {
    start(Op::Put, std::move(key), std::move(value), std::move(done));
}

void Client::append(std::string key, std::string value, Callback done) {
    start(Op::Append, std::move(key), std::move(value), std::move(done));
}

void Client::start(Op op, std::string key, std::string value, Callback done) {
    if (busy()) throw std::logic_error("kv::Client: one request at a time");
    outstanding_ = Command{.client_id = client_id_, .seq = next_seq_++, .op = op,
                           .key = std::move(key), .value = std::move(value)};
    done_ = std::move(done);
    send_now();
}

void Client::on_message(const raft::Message& m) {
    auto r = parse_reply(m);
    if (!r) {
        env_.trace(std::format("dropped message from {}: {}", m.from, r.error()));
        return;
    }
    // Replies to an earlier seq, or duplicates of one already handled, are ignored.
    if (!outstanding_ || r->client_id != client_id_ || r->seq != outstanding_->seq) return;

    if (r->status == Status::Ok) {
        cancel_timers();
        outstanding_.reset();
        ++stats_.completed;
        Callback done = std::move(done_);
        done_ = nullptr;
        if (done) done(r->result);   // may start the next request
        return;
    }

    ++stats_.not_leader;
    const auto& servers = config_.servers;
    if (r->leader_hint && *r->leader_hint != m.from) {
        if (auto it = std::ranges::find(servers, *r->leader_hint); it != servers.end()) {
            target_ = static_cast<std::size_t>(it - servers.begin());
            send_now();
            return;
        }
    }
    // No useful hint (an election is probably under way): wait a little, then try the next.
    try_next_server();
    cancel_timers();
    backoff_timer_ = env_.after(config_.retry_backoff, kBackoff);
}

void Client::on_timer(raft::TimerId id, raft::TimerTag tag) {
    if (!outstanding_) return;
    if (tag == kTimeout && timeout_timer_ == id) {
        timeout_timer_.reset();
        ++stats_.timeouts;
        try_next_server();
        send_now();
    } else if (tag == kBackoff && backoff_timer_ == id) {
        backoff_timer_.reset();
        send_now();
    }
}

void Client::send_now() {
    cancel_timers();
    env_.send(encode_request(config_.servers[target_], *outstanding_));
    timeout_timer_ = env_.after(config_.request_timeout, kTimeout);
}

void Client::try_next_server() { target_ = (target_ + 1) % config_.servers.size(); }

void Client::cancel_timers() {
    if (timeout_timer_) env_.cancel(*timeout_timer_);
    if (backoff_timer_) env_.cancel(*backoff_timer_);
    timeout_timer_.reset();
    backoff_timer_.reset();
}

}  // namespace raftkv::kv
