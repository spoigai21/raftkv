#include "kv/messages.hpp"

#include <format>

#include "kv/kv.pb.h"

namespace raftkv::kv {

namespace {

pb::Op to_pb(Op op) {
    switch (op) {
        case Op::Get: return pb::GET;
        case Op::Put: return pb::PUT;
        case Op::Append: return pb::APPEND;
    }
    return pb::OP_UNSPECIFIED;
}

void fill(pb::Command& out, const Command& c) {
    out.set_client_id(c.client_id);
    out.set_seq(c.seq);
    out.set_op(to_pb(c.op));
    out.set_key(c.key);
    out.set_value(c.value);
}

tl::expected<Command, std::string> from_pb(const pb::Command& pb) {
    Command c{.client_id = pb.client_id(), .seq = pb.seq(), .op = Op::Get, .key = pb.key(),
              .value = pb.value()};
    switch (pb.op()) {
        case pb::GET: c.op = Op::Get; break;
        case pb::PUT: c.op = Op::Put; break;
        case pb::APPEND: c.op = Op::Append; break;
        default: return tl::unexpected(std::string("command has no valid op"));
    }
    if (c.client_id == 0 || c.seq == 0) return tl::unexpected(std::string("command lacks client_id or seq"));
    return c;
}

}  // namespace

const char* to_string(Op op) {
    switch (op) {
        case Op::Get: return "Get";
        case Op::Put: return "Put";
        case Op::Append: return "Append";
    }
    return "?";
}

std::string encode_command(const Command& c) {
    pb::Command pb;
    fill(pb, c);
    return pb.SerializeAsString();
}

tl::expected<Command, std::string> decode_command(const std::string& bytes) {
    pb::Command pb;
    if (!pb.ParseFromString(bytes)) return tl::unexpected(std::string("malformed command"));
    return from_pb(pb);
}

raft::Message encode_request(raft::NodeId to, const Command& c) {
    pb::KvRequest pb;
    fill(*pb.mutable_command(), c);
    return {.from = 0, .to = to, .method = std::string(kKvRequest), .payload = pb.SerializeAsString()};
}

tl::expected<Command, std::string> parse_request(const raft::Message& m) {
    pb::KvRequest pb;
    if (m.method != kKvRequest || !pb.ParseFromString(m.payload)) {
        return tl::unexpected(std::string("malformed KvRequest"));
    }
    return from_pb(pb.command());
}

raft::Message encode_reply(raft::NodeId to, const KvReply& r) {
    pb::KvReply pb;
    pb.set_client_id(r.client_id);
    pb.set_seq(r.seq);
    pb.set_status(r.status == Status::Ok ? pb::OK : pb::NOT_LEADER);
    pb.set_value(r.result.value);
    pb.set_found(r.result.found);
    pb.set_leader_hint(r.leader_hint.value_or(0));
    return {.from = 0, .to = to, .method = std::string(kKvReply), .payload = pb.SerializeAsString()};
}

tl::expected<KvReply, std::string> parse_reply(const raft::Message& m) {
    pb::KvReply pb;
    if (m.method != kKvReply || !pb.ParseFromString(m.payload)) {
        return tl::unexpected(std::string("malformed KvReply"));
    }
    if (pb.status() != pb::OK && pb.status() != pb::NOT_LEADER) {
        return tl::unexpected(std::string("KvReply has no valid status"));
    }
    return KvReply{.client_id = pb.client_id(),
                   .seq = pb.seq(),
                   .status = pb.status() == pb::OK ? Status::Ok : Status::NotLeader,
                   .result = {.value = pb.value(), .found = pb.found()},
                   .leader_hint = pb.leader_hint() ? std::optional(pb.leader_hint()) : std::nullopt};
}

bool is_kv_message(const raft::Message& m) { return m.method == kKvRequest || m.method == kKvReply; }

std::string describe(const raft::Message& m) {
    if (m.method == kKvRequest) {
        auto c = parse_request(m);
        if (!c) return std::format("<{}>", c.error());
        return std::format("KvRequest c{}#{} {} {}", c->client_id % 10000, c->seq, to_string(c->op), c->key);
    }
    auto r = parse_reply(m);
    if (!r) return std::format("<{}>", r.error());
    if (r->status == Status::NotLeader) {
        return std::format("KvReply c{}#{} NotLeader hint={}", r->client_id % 10000, r->seq,
                           r->leader_hint ? std::to_string(*r->leader_hint) : "-");
    }
    return std::format("KvReply c{}#{} ok len={}", r->client_id % 10000, r->seq, r->result.value.size());
}

}  // namespace raftkv::kv
