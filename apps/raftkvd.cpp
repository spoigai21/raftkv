// raftkvd: one raftkv server process.
//
//   raftkvd --id 1 --cluster cluster.json --data-dir data/1 [--snapshot-every N]
//           [--max-batch N] [--unsafe-no-fsync]
//
// --snapshot-every N (default 1000) compacts the log after every N applied entries; 0 keeps
// the whole log forever. --max-batch N caps entries per AppendEntries (default 64; 1 turns
// batching off). --unsafe-no-fsync skips every fsync: data survives a crashed process but
// not a crashed machine. It exists only to measure what fsync costs.
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
    std::fprintf(stderr,
                 "usage: raftkvd --id N --cluster FILE --data-dir DIR [--snapshot-every N] [--max-batch N]\n"
                 "               [--unsafe-no-fsync]\n");
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace raftkv;
    std::uint32_t id = 0;
    std::uint64_t snapshot_every = 1000;
    std::size_t max_batch = 64;
    bool no_fsync = false;
    std::string cluster_file, data_dir;
    for (int i = 1; i < argc; ++i) {
        const std::string_view flag = argv[i];
        if (flag == "--unsafe-no-fsync") {
            no_fsync = true;
            continue;
        }
        if (i + 1 >= argc) return usage();
        const char* value = argv[++i];
        const auto number = [value](auto& out) {
            return std::from_chars(value, value + std::strlen(value), out).ec == std::errc{};
        };
        if (flag == "--id") {
            if (!number(id)) return usage();
        } else if (flag == "--cluster") {
            cluster_file = value;
        } else if (flag == "--data-dir") {
            data_dir = value;
        } else if (flag == "--snapshot-every") {
            if (!number(snapshot_every)) return usage();
        } else if (flag == "--max-batch") {
            if (!number(max_batch) || max_batch == 0) return usage();
        } else {
            return usage();
        }
    }
    if (id == 0 || cluster_file.empty() || data_dir.empty()) return usage();

    auto cluster = net::load_cluster(cluster_file);
    if (!cluster) {
        std::fprintf(stderr, "raftkvd: %s\n", cluster.error().c_str());
        return 2;
    }
    if (!cluster->contains(id)) {
        std::fprintf(stderr, "raftkvd: id %u is not in %s\n", id, cluster_file.c_str());
        return 2;
    }

    if (no_fsync) std::fprintf(stderr, "raftkvd: WARNING: --unsafe-no-fsync: a machine crash can lose acknowledged writes\n");
    auto storage = store::FileStorage::open(
        data_dir, no_fsync ? store::FileStorage::Sync::ProcessCrashOnly : store::FileStorage::Sync::Durable);
    if (!storage) {
        std::fprintf(stderr, "raftkvd: refusing to start: %s\n", storage.error().c_str());
        return 1;
    }

    raft::RaftConfig config{.id = id, .peers = {}};
    config.snapshot_every = snapshot_every;
    config.max_entries_per_append = max_batch;
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
