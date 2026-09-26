#pragma once

#include <filesystem>
#include <map>
#include <string>

#include <asio/ip/tcp.hpp>
#include <tl/expected.hpp>

#include "raft/types.hpp"

namespace raftkv::net {

// Reads a cluster file mapping server id to address, the same file for every node:
//
//   { "1": "127.0.0.1:7001", "2": "127.0.0.1:7002", "3": "127.0.0.1:7003" }
//
// Addresses must be numeric IPs (no DNS lookups). Ids must be 1..2^31-1: ids with the top
// bit set are left for clients.
using ClusterMap = std::map<raft::NodeId, asio::ip::tcp::endpoint>;

tl::expected<ClusterMap, std::string> parse_cluster(const std::string& json_text);
tl::expected<ClusterMap, std::string> load_cluster(const std::filesystem::path& file);

}  // namespace raftkv::net
