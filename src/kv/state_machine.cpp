#include "kv/state_machine.hpp"

#include "kv/kv.pb.h"

namespace raftkv::kv {

Result StateMachine::apply(const Command& c) {
    Session& s = sessions_[c.client_id];
    if (c.seq <= s.last_seq) {
        // Already applied. For the latest seq, the client may still be waiting: send the same
        // reply again. An older seq's reply was already received, or the client would not have
        // moved on, so whatever we return is ignored.
        ++duplicates_;
        return s.last_result;
    }

    Result r;
    switch (c.op) {
        case Op::Get:
            if (auto it = data_.find(c.key); it != data_.end()) r = {.value = it->second, .found = true};
            break;
        case Op::Put:
            data_[c.key] = c.value;
            break;
        case Op::Append:
            data_[c.key] += c.value;
            break;
    }
    s.last_seq = c.seq;
    s.last_result = r;
    return r;
}

std::string StateMachine::serialize() const {
    pb::KvSnapshot pb;
    for (const auto& [key, value] : data_) {
        auto* p = pb.add_data();
        p->set_key(key);
        p->set_value(value);
    }
    for (const auto& [client, s] : sessions_) {
        auto* p = pb.add_sessions();
        p->set_client_id(client);
        p->set_last_seq(s.last_seq);
        p->set_last_value(s.last_result.value);
        p->set_last_found(s.last_result.found);
    }
    return pb.SerializeAsString();
}

tl::expected<StateMachine, std::string> StateMachine::deserialize(const std::string& bytes) {
    pb::KvSnapshot pb;
    if (!pb.ParseFromString(bytes)) return tl::unexpected(std::string("malformed KV snapshot"));
    StateMachine sm;
    for (const auto& p : pb.data()) sm.data_[p.key()] = p.value();
    for (const auto& p : pb.sessions()) {
        sm.sessions_[p.client_id()] = {.last_seq = p.last_seq(),
                                       .last_result = {.value = p.last_value(), .found = p.last_found()}};
    }
    return sm;
}

std::optional<Result> StateMachine::applied_reply(std::uint64_t client_id, std::uint64_t seq) const {
    auto it = sessions_.find(client_id);
    if (it == sessions_.end() || it->second.last_seq != seq) return std::nullopt;
    return it->second.last_result;
}

std::optional<std::string> StateMachine::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

}  // namespace raftkv::kv
