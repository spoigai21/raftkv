// raftkvd: one raftkv server process.
//
//   raftkvd --id 1 --cluster cluster.json --data-dir data/1
//
// Runs kv::Server (Raft + the key-value state machine) on FileStorage, talking to its peers
// over TCP. Logs to stderr. Exits non-zero if its data directory is damaged (it will not
// serve a shortened log) or if a write to disk fails.

#include <charconv>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

#include <asio.hpp>

#include "kv/server.hpp"
#include "net/asio_env.hpp"
#include "net/cluster_config.hpp"
#include "store/file_storage.hpp"
#include "version.hpp"

namespace {

int usage() {
    std::fprintf(stderr, "usage: raftkvd --id N --cluster FILE --data-dir DIR\n");
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace raftkv;
    std::uint32_t id = 0;
    std::string cluster_file, data_dir;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string_view flag = argv[i];
        const char* value = argv[i + 1];
        if (flag == "--id") {
            if (std::from_chars(value, value + std::strlen(value), id).ec != std::errc{}) return usage();
        } else if (flag == "--cluster") {
            cluster_file = value;
        } else if (flag == "--data-dir") {
            data_dir = value;
        } else {
            return usage();
        }
    }
    if (argc % 2 == 0 || id == 0 || cluster_file.empty() || data_dir.empty()) return usage();

    auto cluster = net::load_cluster(cluster_file);
    if (!cluster) {
        std::fprintf(stderr, "raftkvd: %s\n", cluster.error().c_str());
        return 2;
    }
    if (!cluster->contains(id)) {
        std::fprintf(stderr, "raftkvd: id %u is not in %s\n", id, cluster_file.c_str());
        return 2;
    }

    auto storage = store::FileStorage::open(data_dir);
    if (!storage) {
        std::fprintf(stderr, "raftkvd: refusing to start: %s\n", storage.error().c_str());
        return 1;
    }

    raft::RaftConfig config{.id = id, .peers = {}};
    for (const auto& [peer, endpoint] : *cluster) {
        if (peer != id) config.peers.push_back(peer);
    }

    asio::io_context io;
    net::AsioEnv env(io, {.id = id, .servers = *cluster, .listen = true}, storage->get());
    kv::Server server(config, env);

    asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](asio::error_code, int) { env.stop(); });

    try {
        env.start(server);
        std::fprintf(stderr, "raftkvd %.*s: node %u listening on %s, data in %s\n",
                     static_cast<int>(version().size()), version().data(), id,
                     cluster->at(id).address().to_string().c_str(), data_dir.c_str());
        io.run();
    } catch (const std::exception& e) {
        // A storage failure lands here: after a failed fsync the only safe thing is to stop.
        std::fprintf(stderr, "raftkvd: fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
