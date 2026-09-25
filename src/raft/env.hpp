#pragma once

#include <cstdint>
#include <string_view>

#include "raft/storage.hpp"
#include "raft/types.hpp"

namespace raftkv::raft {

// Everything nondeterministic that a node needs, injected (implementation guide §3.2).
// Code under src/raft and src/kv reaches time, timers, the network, disk and randomness
// only through this interface.
class Env {
public:
    virtual ~Env() = default;

    virtual Time now() const = 0;

    // Schedules Node::on_timer(id, tag) after `delay`. Timers do not survive a crash.
    virtual TimerId after(Duration delay, TimerTag tag) = 0;
    // Cancelling a timer that already fired or was already cancelled is a no-op.
    virtual void cancel(TimerId id) = 0;

    // Fire and forget: the network may drop, delay or reorder it.
    virtual void send(Message m) = 0;

    virtual Storage& storage() = 0;

    // Uniform 64-bit value from a seeded stream in the simulator.
    virtual std::uint64_t random() = 0;

    // A human-readable note for the event log ("became leader term 3"). No effect on behaviour.
    virtual void trace(std::string_view what) = 0;
};

// What the environment drives. A node never blocks and never runs on more than one
// thread at a time (implementation guide §3.1).
class Node {
public:
    virtual ~Node() = default;

    // Called once when the node boots or restarts after a crash; it reloads from
    // env.storage() here and schedules its first timers.
    virtual void on_start() = 0;
    virtual void on_message(const Message& m) = 0;
    virtual void on_timer(TimerId id, TimerTag tag) = 0;
};

}  // namespace raftkv::raft
