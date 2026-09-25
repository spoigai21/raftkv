#include "kv/state_machine.hpp"

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

std::optional<std::string> StateMachine::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

}  // namespace raftkv::kv
