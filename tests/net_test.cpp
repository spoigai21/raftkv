#include <gtest/gtest.h>

#include <asio.hpp>

#include "net/asio_env.hpp"
#include "net/cluster_config.hpp"
#include "net/frame.hpp"
#include "sim/rng.hpp"
#include "sim_test_nodes.hpp"

namespace raftkv {
namespace {

using raft::Message;

std::span<const std::byte> as_bytes(const std::string& s) { return std::as_bytes(std::span(s)); }

bool same(const Message& a, const Message& b) {
    return a.from == b.from && a.to == b.to && a.method == b.method && a.payload == b.payload;
}

std::vector<Message> sample() {
    return {{.from = 1, .to = 2, .method = "RequestVote", .payload = "abc"},
            {.from = 2, .to = 1, .method = "", .payload = ""},
            {.from = 0x80000007u, .to = 3, .method = "KvRequest", .payload = std::string(100'000, 'x')},
            {.from = 3, .to = 1, .method = "AppendEntries", .payload = std::string("\0\1\2", 3)}};
}

TEST(Frame, DecodesWhateverWayTheStreamIsChopped) {
    std::string stream;
    for (const Message& m : sample()) stream += net::encode_frame(m);
    sim::Rng rng(5);
    for (int trial = 0; trial < 200; ++trial) {
        net::FrameReader reader;
        std::vector<Message> got;
        std::size_t pos = 0;
        while (pos < stream.size()) {
            // Chunks from 1 byte (worst case) to 64 KiB.
            const std::size_t n = std::min<std::size_t>(stream.size() - pos, rng.between(1, trial % 2 ? 7 : 65536));
            reader.feed(as_bytes(stream).subspan(pos, n));
            pos += n;
            while (true) {
                auto m = reader.next();
                ASSERT_TRUE(m.has_value()) << m.error();
                if (!*m) break;
                got.push_back(std::move(**m));
            }
        }
        ASSERT_EQ(got.size(), sample().size());
        for (std::size_t i = 0; i < got.size(); ++i) EXPECT_TRUE(same(got[i], sample()[i])) << "message " << i;
        EXPECT_EQ(reader.buffered(), 0u);
    }
}

TEST(Frame, OversizedLengthIsRejectedWithoutWaitingForTheBody) {
    net::FrameReader reader;
    const std::string header("\xff\xff\xff\x7f", 4);   // ~2 GiB
    reader.feed(as_bytes(header));
    auto m = reader.next();
    ASSERT_FALSE(m.has_value());
    EXPECT_NE(m.error().find("exceeds"), std::string::npos);
    EXPECT_FALSE(reader.next().has_value()) << "the error is sticky: framing is lost for good";
}

TEST(Frame, GarbageBodyIsAnError) {
    net::FrameReader reader;
    const std::string frame = std::string("\x03\x00\x00\x00", 4) + "\xff\xff\xff";
    reader.feed(as_bytes(frame));
    EXPECT_FALSE(reader.next().has_value());
}

TEST(ClusterConfig, ParsesAValidFile) {
    auto c = net::parse_cluster(R"({"1":"127.0.0.1:7001","2":"10.0.0.2:7002","3":"::1:7003"})");
    ASSERT_TRUE(c.has_value()) << c.error();
    ASSERT_EQ(c->size(), 3u);
    EXPECT_EQ(c->at(1).port(), 7001);
    EXPECT_EQ(c->at(2).address().to_string(), "10.0.0.2");
    EXPECT_TRUE(c->at(3).address().is_v6());
}

TEST(ClusterConfig, RejectsBadInput) {
    for (const char* bad : {"", "[]", "{}", R"({"0":"127.0.0.1:1"})", R"({"x":"127.0.0.1:1"})",
                            R"({"1":"127.0.0.1"})", R"({"1":"127.0.0.1:0"})", R"({"1":"127.0.0.1:70000"})",
                            R"({"1":"example.com:80"})", R"({"1":7001})", R"({"2147483648":"127.0.0.1:1"})"}) {
        EXPECT_FALSE(net::parse_cluster(bad).has_value()) << bad;
    }
}

// Two AsioEnvs in one process over real loopback TCP: a server that listens, and a client
// that dials it and gets a reply on the same connection.
TEST(AsioEnv, ClientAndServerExchangeMessagesOverTcp) {
    asio::io_context io;
    net::NetConfig server_cfg{.id = 1, .servers = {{1, {asio::ip::make_address("127.0.0.1"), 0}}}, .listen = true};
    net::AsioEnv server_env(io, server_cfg, nullptr);
    test::ProbeNode server(server_env);
    server_env.start(server);

    net::NetConfig client_cfg{.id = 0x80000001u,
                              .servers = {{1, {asio::ip::make_address("127.0.0.1"), server_env.listening_port()}}},
                              .listen = false};
    net::AsioEnv client_env(io, client_cfg, nullptr);
    test::ProbeNode client(client_env);
    client_env.start(client);

    // The client sends; the server answers whoever wrote to it.
    asio::post(io, [&] { client_env.send({.from = 0, .to = 1, .method = "Ping", .payload = "hi"}); });
    asio::steady_timer poll(io);
    std::function<void()> check = [&] {
        if (server.inbox.size() == 1 && client.inbox.empty()) {
            server_env.send({.from = 0, .to = server.inbox[0].from, .method = "Pong", .payload = "yo"});
        }
        if (!client.inbox.empty()) {
            client_env.stop();
            server_env.stop();
            return;
        }
        poll.expires_after(std::chrono::milliseconds(1));
        poll.async_wait([&](asio::error_code ec) { if (!ec) check(); });
    };
    check();
    io.run_for(std::chrono::seconds(5));

    ASSERT_EQ(server.inbox.size(), 1u);
    EXPECT_EQ(server.inbox[0].from, 0x80000001u) << "the sender id is filled in by the Env";
    EXPECT_EQ(server.inbox[0].payload, "hi");
    ASSERT_EQ(client.inbox.size(), 1u);
    EXPECT_EQ(client.inbox[0].method, "Pong");
    EXPECT_EQ(client.inbox[0].from, 1u);
}

// Regression test for a crash found by the Phase 7 repeat runs: when the peer vanished, the
// read side closed the connection and cleared the outgoing queue while a write was still in
// flight; the write then completed and popped from the empty queue (undefined behaviour).
// Here a client keeps small writes completing while the server end is closed under it, so a
// successful write completion and the read side's error land together.
TEST(AsioEnv, PeerClosingWhileWritesAreInFlightIsSafe) {
    for (int round = 0; round < 300; ++round) {
        asio::io_context io;
        net::NetConfig server_cfg{.id = 1, .servers = {{1, {asio::ip::make_address("127.0.0.1"), 0}}}, .listen = true};
        auto server_env = std::make_unique<net::AsioEnv>(io, server_cfg, nullptr);
        test::ProbeNode server(*server_env);
        server_env->start(server);

        net::NetConfig client_cfg{.id = 0x80000001u,
                                  .servers = {{1, {asio::ip::make_address("127.0.0.1"), server_env->listening_port()}}},
                                  .listen = false};
        net::AsioEnv client_env(io, client_cfg, nullptr);
        test::ProbeNode client(client_env);
        client_env.start(client);

        asio::steady_timer t(io);
        std::function<void(int)> pump = [&](int n) {
            for (int i = 0; i < 4; ++i) client_env.send({.from = 0, .to = 1, .method = "Ping", .payload = "x"});
            if (n == 5 + round % 7) server_env->stop();   // the peer vanishes mid-stream
            if (n == 20) {
                client_env.stop();
                return;
            }
            t.expires_after(std::chrono::microseconds(100 * (1 + round % 5)));
            t.async_wait([&, n](asio::error_code ec) { if (!ec) pump(n + 1); });
        };
        asio::post(io, [&] { pump(0); });
        io.run_for(std::chrono::seconds(5));
    }
    SUCCEED() << "no crash and no sanitizer report";
}

TEST(AsioEnv, TimersFireAndCancel) {
    asio::io_context io;
    net::AsioEnv env(io, {.id = 1, .servers = {}, .listen = false}, nullptr);
    test::ProbeNode node(env);
    env.start(node);
    env.after(std::chrono::milliseconds(20), 2);
    const auto cancelled = env.after(std::chrono::milliseconds(10), 99);
    env.after(std::chrono::milliseconds(5), 1);
    env.cancel(cancelled);
    io.run_for(std::chrono::milliseconds(200));
    EXPECT_EQ(node.timers, (std::vector<raft::TimerTag>{1, 2}));
    EXPECT_GE(env.now().since_start, std::chrono::milliseconds(20));
}

}  // namespace
}  // namespace raftkv
