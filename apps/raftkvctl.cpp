// raftkvctl: command-line client.
//
//   raftkvctl --cluster cluster.json get KEY
//   raftkvctl --cluster cluster.json put KEY VALUE
//   raftkvctl --cluster cluster.json append KEY VALUE
//
// Options: --timeout-ms N (default 10000) gives up after N ms; --verbose prints which server
// answered and how many retries it took.
//
// Exit codes: 0 done (get: prints the value), 1 key not found (get), 2 usage, 3 timed out.

#include <charconv>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <asio.hpp>

#include "kv/client.hpp"
#include "net/asio_env.hpp"
#include "net/cluster_config.hpp"

namespace {

int usage() {
    std::fprintf(stderr,
                 "usage: raftkvctl --cluster FILE [--timeout-ms N] [--verbose] "
                 "(get KEY | put KEY VALUE | append KEY VALUE)\n");
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace raftkv;
    std::string cluster_file;
    long timeout_ms = 10'000;
    bool verbose = false;
    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i) {
        const std::string_view a = argv[i];
        if (a == "--cluster" && i + 1 < argc) {
            cluster_file = argv[++i];
        } else if (a == "--timeout-ms" && i + 1 < argc) {
            const char* v = argv[++i];
            if (std::from_chars(v, v + std::strlen(v), timeout_ms).ec != std::errc{}) return usage();
        } else if (a == "--verbose") {
            verbose = true;
        } else {
            args.emplace_back(a);
        }
    }
    if (cluster_file.empty() || args.empty()) return usage();
    const std::string& op = args[0];
    const bool ok_shape = (op == "get" && args.size() == 2) ||
                          ((op == "put" || op == "append") && args.size() == 3);
    if (!ok_shape) return usage();

    auto cluster = net::load_cluster(cluster_file);
    if (!cluster) {
        std::fprintf(stderr, "raftkvctl: %s\n", cluster.error().c_str());
        return 2;
    }
    std::vector<raft::NodeId> servers;
    for (const auto& [id, endpoint] : *cluster) servers.push_back(id);

    // Client ids have the top bit set, so they never collide with server ids.
    const auto self = static_cast<raft::NodeId>(0x8000'0000u | (std::random_device{}() & 0x7fff'ffffu));

    asio::io_context io;
    net::AsioEnv env(io, {.id = self, .servers = *cluster, .listen = false}, nullptr);
    kv::Client client({.servers = servers}, env);

    int exit_code = 3;
    asio::steady_timer deadline(io, std::chrono::milliseconds(timeout_ms));
    deadline.async_wait([&](asio::error_code ec) {
        if (ec) return;
        std::fprintf(stderr, "raftkvctl: no answer within %ld ms\n", timeout_ms);
        env.stop();
    });

    auto finish = [&](const kv::Result& r) {
        if (op == "get") {
            if (r.found) {
                std::fwrite(r.value.data(), 1, r.value.size(), stdout);
                std::fputc('\n', stdout);
                exit_code = 0;
            } else {
                std::fprintf(stderr, "raftkvctl: %s not found\n", args[1].c_str());
                exit_code = 1;
            }
        } else {
            std::puts("OK");
            exit_code = 0;
        }
        if (verbose) {
            const auto& s = client.stats();
            std::fprintf(stderr, "served-by %u timeouts %llu not-leader %llu\n", client.current_server(),
                         static_cast<unsigned long long>(s.timeouts),
                         static_cast<unsigned long long>(s.not_leader));
        }
        deadline.cancel();
        env.stop();
    };

    env.start(client);
    asio::post(io, [&] {
        if (op == "get") client.get(args[1], finish);
        else if (op == "put") client.put(args[1], args[2], finish);
        else client.append(args[1], args[2], finish);
    });
    io.run();
    return exit_code;
}
