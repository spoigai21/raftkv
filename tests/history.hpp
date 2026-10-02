#pragma once

// Records client operations for linearizability checking (implementation guide Phase 6).
// Times are the simulator's virtual time, taken at the client: when it starts an operation
// and when the reply arrives. An operation still outstanding when the run stops is recorded
// with no return: it may or may not have taken effect, and the checker treats it that way.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "kv/messages.hpp"
#include "raft/types.hpp"

namespace raftkv::test {

class History {
public:
    std::size_t invoke(raft::NodeId client, kv::Op op, std::string key, std::string value, raft::Time at) {
        ops_.push_back({.client = client, .op = op, .key = std::move(key), .value = std::move(value),
                        .call = at, .ret = std::nullopt, .output = std::nullopt});
        return ops_.size() - 1;
    }

    void complete(std::size_t i, const kv::Result& r, raft::Time at) {
        ops_[i].ret = at;
        ops_[i].output = r;
    }

    std::size_t size() const { return ops_.size(); }

    // Operations that returned within [from, to), from every client or just `client`.
    std::size_t returned_between(raft::Time from, raft::Time to, std::optional<raft::NodeId> client = {}) const {
        std::size_t n = 0;
        for (const auto& op : ops_) {
            n += op.ret && *op.ret >= from && *op.ret < to && (!client || op.client == *client);
        }
        return n;
    }

    // Latencies of operations called within [from, to) that have returned, sorted.
    std::vector<raft::Duration> latencies(raft::Time from, raft::Time to) const {
        std::vector<raft::Duration> out;
        for (const auto& op : ops_) {
            if (op.ret && op.call >= from && op.call < to) out.push_back(*op.ret - op.call);
        }
        std::sort(out.begin(), out.end());
        return out;
    }
    std::size_t pending() const {
        std::size_t n = 0;
        for (const auto& op : ops_) n += !op.ret.has_value();
        return n;
    }

    std::string to_json(std::uint64_t seed, const std::string& variant) const {
        nlohmann::json ops = nlohmann::json::array();
        for (const auto& op : ops_) {
            nlohmann::json j{{"client", op.client},
                             {"op", op.op == kv::Op::Get ? "get" : op.op == kv::Op::Put ? "put" : "append"},
                             {"key", op.key},
                             {"value", op.value},
                             {"call", op.call.since_start.count()},
                             {"return", nullptr},
                             {"output", nullptr}};
            if (op.ret) {
                j["return"] = op.ret->since_start.count();
                j["output"] = {{"value", op.output->value}, {"found", op.output->found}};
            }
            ops.push_back(std::move(j));
        }
        return nlohmann::json{{"seed", seed}, {"variant", variant}, {"ops", std::move(ops)}}.dump();
    }

    // Writes <RAFTKV_HISTORY_DIR>/<variant>-seed<N>.json when that variable is set.
    void write_if_requested(std::uint64_t seed, const std::string& variant) const {
        const char* dir = std::getenv("RAFTKV_HISTORY_DIR");
        if (dir == nullptr) return;
        std::filesystem::create_directories(dir);
        std::ofstream(std::filesystem::path(dir) / std::format("{}-seed{}.json", variant, seed)) << to_json(seed, variant);
    }

private:
    struct Op {
        raft::NodeId client;
        kv::Op op;
        std::string key;
        std::string value;
        raft::Time call;
        std::optional<raft::Time> ret;
        std::optional<kv::Result> output;
    };
    std::vector<Op> ops_;
};

}  // namespace raftkv::test
