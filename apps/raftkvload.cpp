// raftkvload: a load generator for a running raftkv cluster (implementation guide Phase 9).
//
//   raftkvload --cluster FILE [--clients N] [--seconds S] [--warmup-ms MS]
//              [--write-pct P] [--value-bytes B] [--keys K]
//              [--history FILE] [--progress]
//
// --history FILE records every operation for the linearizability checker (tools/lincheck),
// in the same JSON format as the simulator's histories. It switches the workload to
// get/put/append (40/30/30) with unique values, so the checker can tell every write apart.
// Times are microseconds on this host's monotonic clock; an operation still outstanding when
// the run ends is recorded with no return. --progress prints completions per second to stderr.
//
// Runs N clients in one process, each with one request outstanding (closed loop), for S
// seconds after a warmup, and prints one JSON object:
//
//   ops, ops_per_sec, write/read p50 and p99 latency (ms), the longest gap between two
//   completed operations (ms; how long the cluster was unavailable, e.g. across a leader
//   kill), and how many requests timed out or were redirected.
//
// Latency is measured here, on the client host, with a monotonic clock.

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <optional>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <asio.hpp>
#include <nlohmann/json.hpp>

#include "kv/client.hpp"
#include "net/asio_env.hpp"
#include "net/cluster_config.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::string cluster_file;
    int clients = 8;
    double seconds = 10;
    long warmup_ms = 1000;
    int write_pct = 100;
    std::size_t value_bytes = 16;
    int keys = 1000;
    std::string history_file;
    bool progress = false;
};

// One recorded operation, for --history.
struct Recorded {
    std::size_t client;
    const char* op;
    std::string key, value;
    std::int64_t call_us;
    std::optional<std::int64_t> return_us;
    raftkv::kv::Result output;
};

int usage() {
    std::fprintf(stderr,
                 "usage: raftkvload --cluster FILE [--clients N] [--seconds S] [--warmup-ms MS]\n"
                 "                  [--write-pct P] [--value-bytes B] [--keys K] [--history FILE] [--progress]\n");
    return 2;
}

template <class T>
bool parse(const char* s, T& out) {
    return std::from_chars(s, s + std::strlen(s), out).ec == std::errc{};
}

double percentile_ms(std::vector<double>& v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, static_cast<std::size_t>(p * static_cast<double>(v.size())))];
}

}  // namespace

int main(int argc, char** argv) {
    using namespace raftkv;
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string_view f = argv[i];
        if (f == "--progress") {
            opt.progress = true;
            continue;
        }
        if (i + 1 >= argc) return usage();
        const char* v = argv[++i];
        bool ok = true;
        if (f == "--cluster") opt.cluster_file = v;
        else if (f == "--history") opt.history_file = v;
        else if (f == "--clients") ok = parse(v, opt.clients) && opt.clients > 0;
        else if (f == "--seconds") opt.seconds = std::strtod(v, nullptr);
        else if (f == "--warmup-ms") ok = parse(v, opt.warmup_ms);
        else if (f == "--write-pct") ok = parse(v, opt.write_pct) && opt.write_pct >= 0 && opt.write_pct <= 100;
        else if (f == "--value-bytes") ok = parse(v, opt.value_bytes);
        else if (f == "--keys") ok = parse(v, opt.keys) && opt.keys > 0;
        else ok = false;
        if (!ok) return usage();
    }
    if (opt.cluster_file.empty() || opt.seconds <= 0) return usage();
    const bool recording = !opt.history_file.empty();

    auto cluster = net::load_cluster(opt.cluster_file);
    if (!cluster) {
        std::fprintf(stderr, "raftkvload: %s\n", cluster.error().c_str());
        return 2;
    }
    std::vector<raft::NodeId> servers;
    for (const auto& [id, ep] : *cluster) servers.push_back(id);

    asio::io_context io;
    std::mt19937_64 rng(std::random_device{}());
    const std::string value(opt.value_bytes, 'v');
    const auto start = Clock::now();
    const auto measure_from = start + std::chrono::milliseconds(opt.warmup_ms);
    const auto measure_until = measure_from + std::chrono::duration_cast<Clock::duration>(
                                                  std::chrono::duration<double>(opt.seconds));

    std::vector<double> write_ms, read_ms;
    Clock::time_point last_completion{};
    double max_gap_ms = 0;
    std::uint64_t ops = 0;

    struct Worker {
        std::unique_ptr<net::AsioEnv> env;
        std::unique_ptr<kv::Client> client;
    };
    std::vector<Worker> workers;
    for (int i = 0; i < opt.clients; ++i) {
        // Client ids have the top bit set (servers use the rest); random so runs do not clash.
        const auto id = static_cast<raft::NodeId>(0x8000'0000u | (rng() & 0x7fff'ffffu));
        Worker w;
        w.env = std::make_unique<net::AsioEnv>(io, net::NetConfig{.id = id, .servers = *cluster, .listen = false}, nullptr);
        w.client = std::make_unique<kv::Client>(kv::ClientConfig{.servers = servers}, *w.env);
        w.env->start(*w.client);
        workers.push_back(std::move(w));
    }

    std::vector<Recorded> history;
    std::vector<int> issued(workers.size(), 0);
    std::uint64_t this_second = 0;
    const auto us_since_start = [&](Clock::time_point t) {
        return std::chrono::duration_cast<std::chrono::microseconds>(t - start).count();
    };

    std::function<void(std::size_t)> issue = [&](std::size_t wi) {
        if (Clock::now() >= measure_until) return;
        kv::Client& c = *workers[wi].client;
        const std::string key = "k" + std::to_string(rng() % static_cast<std::uint64_t>(opt.keys));
        // Plain load: put or get by --write-pct. Recording: get/put/append with unique values.
        const auto roll = static_cast<int>(rng() % 100);
        const char* op = recording ? (roll < 40 ? "get" : roll < 70 ? "put" : "append")
                                   : (roll < opt.write_pct ? "put" : "get");
        const bool write = std::string_view(op) != "get";
        const std::string v = recording ? std::to_string(wi) + "." + std::to_string(issued[wi]++) + ";" : value;
        const auto t0 = Clock::now();
        std::optional<std::size_t> rec;
        if (recording) {
            history.push_back({.client = wi, .op = op, .key = key, .value = write ? v : "",
                               .call_us = us_since_start(t0), .return_us = std::nullopt, .output = {}});
            rec = history.size() - 1;
        }
        auto done = [&, t0, write, rec, wi](const kv::Result& r) {
            const auto t1 = Clock::now();
            ++this_second;
            if (rec) {
                history[*rec].return_us = us_since_start(t1);
                history[*rec].output = r;
            }
            if (t1 >= measure_from && t1 < measure_until) {
                ++ops;
                (write ? write_ms : read_ms).push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
                if (last_completion >= measure_from) {
                    max_gap_ms = std::max(max_gap_ms, std::chrono::duration<double, std::milli>(t1 - last_completion).count());
                }
                last_completion = t1;
            } else if (t1 < measure_from) {
                last_completion = t1;
            }
            issue(wi);
        };
        if (std::string_view(op) == "put") c.put(key, v, done);
        else if (std::string_view(op) == "append") c.append(key, v, done);
        else c.get(key, done);
    };
    for (std::size_t wi = 0; wi < workers.size(); ++wi) asio::post(io, [&, wi] { issue(wi); });

    asio::steady_timer ticker(io);
    int second = 0;
    std::function<void()> tick = [&] {
        ticker.expires_after(std::chrono::seconds(1));
        ticker.async_wait([&](asio::error_code ec) {
            if (ec || Clock::now() >= measure_until) return;
            std::fprintf(stderr, "  t=%2ds  %6llu ops/s\n", ++second, static_cast<unsigned long long>(this_second));
            this_second = 0;
            tick();
        });
    };
    if (opt.progress) tick();

    // Stop once the window has closed and every client is idle, or 30 s after it closed.
    asio::steady_timer poll(io);
    std::function<void()> check = [&] {
        const bool idle = std::ranges::none_of(workers, [](const Worker& w) { return w.client->busy(); });
        if ((Clock::now() >= measure_until && idle) || Clock::now() >= measure_until + std::chrono::seconds(30)) {
            ticker.cancel();
            for (auto& w : workers) w.env->stop();
            return;
        }
        poll.expires_after(std::chrono::milliseconds(20));
        poll.async_wait([&](asio::error_code ec) { if (!ec) check(); });
    };
    check();
    io.run();

    if (recording) {
        nlohmann::json ops_json = nlohmann::json::array();
        for (const Recorded& r : history) {
            nlohmann::json j{{"client", r.client}, {"op", r.op},          {"key", r.key},      {"value", r.value},
                             {"call", r.call_us}, {"return", nullptr}, {"output", nullptr}};
            if (r.return_us) {
                j["return"] = *r.return_us;
                j["output"] = {{"value", r.output.value}, {"found", r.output.found}};
            }
            ops_json.push_back(std::move(j));
        }
        std::ofstream(opt.history_file) << nlohmann::json{{"seed", 0}, {"variant", "real-processes"}, {"ops", ops_json}}.dump();
    }

    std::uint64_t timeouts = 0, not_leader = 0;
    for (const auto& w : workers) {
        timeouts += w.client->stats().timeouts;
        not_leader += w.client->stats().not_leader;
    }
    std::printf("{\"clients\": %d, \"seconds\": %.1f, \"write_pct\": %d, \"value_bytes\": %zu, \"ops\": %llu, "
                "\"ops_per_sec\": %.1f, \"write_p50_ms\": %.3f, \"write_p99_ms\": %.3f, \"read_p50_ms\": %.3f, "
                "\"read_p99_ms\": %.3f, \"max_gap_ms\": %.1f, \"timeouts\": %llu, \"not_leader\": %llu}\n",
                opt.clients, opt.seconds, opt.write_pct, opt.value_bytes, static_cast<unsigned long long>(ops),
                static_cast<double>(ops) / opt.seconds, percentile_ms(write_ms, 0.50), percentile_ms(write_ms, 0.99),
                percentile_ms(read_ms, 0.50), percentile_ms(read_ms, 0.99), max_gap_ms,
                static_cast<unsigned long long>(timeouts), static_cast<unsigned long long>(not_leader));
    return ops > 0 ? 0 : 1;
}
