#pragma once

#include <cstdint>
#include <map>

#include "kv/messages.hpp"
#include "kv/state_machine.hpp"
#include "raft/raft.hpp"

namespace raftkv::kv {

// One key-value server: a Raft node plus the state machine it drives. Client requests go
// through the log (reads too, so they are linearizable), and the reply is sent once the
// entry is applied.
class Server final : public raft::Node {
public:
    // `observer` sees every applied entry too; tests use it to check invariants.
    Server(raft::RaftConfig config, raft::Env& env, raft::ApplyFn observer = {});

    void on_start() override;
    void on_message(const raft::Message& m) override;
    void on_timer(raft::TimerId id, raft::TimerTag tag) override;

    raft::Raft& raft() { return raft_; }
    const raft::Raft& raft() const { return raft_; }
    const StateMachine& state() const { return state_; }

private:
    struct Pending {
        raft::NodeId from = 0;   // the client node to reply to
        std::uint64_t client_id = 0;
        std::uint64_t seq = 0;
        raft::Term term = 0;
    };

    void handle_request(raft::NodeId from, const Command& c);
    void on_apply(const raft::LogEntry& e);
    // A leader that lost leadership fails what it was waiting on, rather than leaving
    // clients to time out. The client retries elsewhere with the same seq.
    void fail_pending_if_deposed();
    void reply(raft::NodeId to, KvReply r);

    raft::Env& env_;
    raft::ApplyFn observer_;
    raft::Raft raft_;
    StateMachine state_;
    std::map<raft::Index, Pending> pending_;   // leader only: proposed, not yet applied
};

}  // namespace raftkv::kv
