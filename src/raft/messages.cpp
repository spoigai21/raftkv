#include "raft/messages.hpp"

#include <format>
#include <string_view>

#include "raft/messages.pb.h"

namespace raftkv::raft {

namespace {

constexpr std::string_view kRequestVote = "RequestVote";
constexpr std::string_view kRequestVoteReply = "RequestVoteReply";
constexpr std::string_view kAppendEntries = "AppendEntries";
constexpr std::string_view kAppendEntriesReply = "AppendEntriesReply";

std::string serialize(const google::protobuf::MessageLite& pb) {
    std::string out;
    pb.SerializeToString(&out);
    return out;
}

struct Encoder {
    NodeId to;

    Message operator()(const RequestVote& r) const {
        pb::RequestVote pb;
        pb.set_term(r.term);
        pb.set_candidate_id(r.candidate_id);
        pb.set_last_log_index(r.last_log_index);
        pb.set_last_log_term(r.last_log_term);
        return {.from = 0, .to = to, .method = std::string(kRequestVote), .payload = serialize(pb)};
    }
    Message operator()(const RequestVoteReply& r) const {
        pb::RequestVoteReply pb;
        pb.set_term(r.term);
        pb.set_vote_granted(r.vote_granted);
        return {.from = 0, .to = to, .method = std::string(kRequestVoteReply), .payload = serialize(pb)};
    }
    Message operator()(const AppendEntries& r) const {
        pb::AppendEntries pb;
        pb.set_term(r.term);
        pb.set_leader_id(r.leader_id);
        pb.set_prev_log_index(r.prev_log_index);
        pb.set_prev_log_term(r.prev_log_term);
        for (const LogEntry& e : r.entries) {
            pb::Entry* out = pb.add_entries();
            out->set_term(e.term);
            out->set_index(e.index);
            out->set_command(e.command);
        }
        pb.set_leader_commit(r.leader_commit);
        return {.from = 0, .to = to, .method = std::string(kAppendEntries), .payload = serialize(pb)};
    }
    Message operator()(const AppendEntriesReply& r) const {
        pb::AppendEntriesReply pb;
        pb.set_term(r.term);
        pb.set_success(r.success);
        pb.set_match_index(r.match_index);
        pb.set_conflict_index(r.conflict_index);
        pb.set_conflict_term(r.conflict_term);
        return {.from = 0, .to = to, .method = std::string(kAppendEntriesReply), .payload = serialize(pb)};
    }
};

template <class Pb>
tl::expected<Pb, std::string> decode_pb(const Message& m) {
    Pb pb;
    if (!pb.ParseFromString(m.payload)) {
        return tl::unexpected(std::format("malformed {} ({} bytes)", m.method, m.payload.size()));
    }
    return pb;
}

}  // namespace

Message encode(NodeId to, const Rpc& rpc) { return std::visit(Encoder{to}, rpc); }

tl::expected<Rpc, std::string> parse(const Message& m) {
    if (m.method == kRequestVote) {
        return decode_pb<pb::RequestVote>(m).map([](const pb::RequestVote& pb) -> Rpc {
            return RequestVote{.term = pb.term(), .candidate_id = pb.candidate_id(),
                               .last_log_index = pb.last_log_index(),
                               .last_log_term = pb.last_log_term()};
        });
    }
    if (m.method == kRequestVoteReply) {
        return decode_pb<pb::RequestVoteReply>(m).map([](const pb::RequestVoteReply& pb) -> Rpc {
            return RequestVoteReply{.term = pb.term(), .vote_granted = pb.vote_granted()};
        });
    }
    if (m.method == kAppendEntries) {
        return decode_pb<pb::AppendEntries>(m).map([](const pb::AppendEntries& pb) -> Rpc {
            AppendEntries r{.term = pb.term(), .leader_id = pb.leader_id(),
                            .prev_log_index = pb.prev_log_index(),
                            .prev_log_term = pb.prev_log_term(), .entries = {},
                            .leader_commit = pb.leader_commit()};
            r.entries.reserve(static_cast<std::size_t>(pb.entries_size()));
            for (const pb::Entry& e : pb.entries()) {
                r.entries.push_back({.term = e.term(), .index = e.index(), .command = e.command()});
            }
            return r;
        });
    }
    if (m.method == kAppendEntriesReply) {
        return decode_pb<pb::AppendEntriesReply>(m).map([](const pb::AppendEntriesReply& pb) -> Rpc {
            return AppendEntriesReply{.term = pb.term(), .success = pb.success(),
                                      .match_index = pb.match_index(),
                                      .conflict_index = pb.conflict_index(),
                                      .conflict_term = pb.conflict_term()};
        });
    }
    return tl::unexpected(std::format("unknown method '{}'", m.method));
}

namespace {

struct Describer {
    std::string operator()(const RequestVote& r) const {
        return std::format("RequestVote term={} last={}/{}", r.term, r.last_log_index, r.last_log_term);
    }
    std::string operator()(const RequestVoteReply& r) const {
        return std::format("RequestVoteReply term={} granted={}", r.term, r.vote_granted);
    }
    std::string operator()(const AppendEntries& r) const {
        return std::format("AppendEntries term={} prev={}/{} n={} commit={}", r.term,
                           r.prev_log_index, r.prev_log_term, r.entries.size(), r.leader_commit);
    }
    std::string operator()(const AppendEntriesReply& r) const {
        return r.success ? std::format("AppendEntriesReply term={} ok match={}", r.term, r.match_index)
                         : std::format("AppendEntriesReply term={} fail conflict={}/{}", r.term,
                                       r.conflict_index, r.conflict_term);
    }
};

}  // namespace

std::string describe(const Message& m) {
    auto rpc = parse(m);
    return rpc ? std::visit(Describer{}, *rpc) : std::format("<{}>", rpc.error());
}

}  // namespace raftkv::raft
