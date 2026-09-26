#include "net/cluster_config.hpp"

#include <charconv>
#include <format>
#include <fstream>
#include <sstream>

#include <nlohmann/json.hpp>

namespace raftkv::net {

tl::expected<ClusterMap, std::string> parse_cluster(const std::string& json_text) {
    const auto json = nlohmann::json::parse(json_text, nullptr, /*allow_exceptions=*/false);
    if (json.is_discarded() || !json.is_object()) return tl::unexpected(std::string("not a JSON object"));
    if (json.empty()) return tl::unexpected(std::string("no servers"));

    ClusterMap out;
    for (const auto& [key, value] : json.items()) {
        std::uint64_t id = 0;
        const auto [end, ec] = std::from_chars(key.data(), key.data() + key.size(), id);
        if (ec != std::errc{} || end != key.data() + key.size() || id == 0 || id >= (1ull << 31)) {
            return tl::unexpected(std::format("bad server id '{}'", key));
        }
        if (!value.is_string()) return tl::unexpected(std::format("server {}: address must be a string", key));
        const std::string addr = value.get<std::string>();
        const auto colon = addr.rfind(':');
        std::uint32_t port = 0;
        if (colon == std::string::npos ||
            std::from_chars(addr.data() + colon + 1, addr.data() + addr.size(), port).ec != std::errc{} ||
            port == 0 || port > 65535) {
            return tl::unexpected(std::format("server {}: expected ip:port, got '{}'", key, addr));
        }
        asio::error_code ec2;
        const auto ip = asio::ip::make_address(addr.substr(0, colon), ec2);
        if (ec2) return tl::unexpected(std::format("server {}: bad ip in '{}'", key, addr));
        out.emplace(static_cast<raft::NodeId>(id), asio::ip::tcp::endpoint(ip, static_cast<std::uint16_t>(port)));
    }
    return out;
}

tl::expected<ClusterMap, std::string> load_cluster(const std::filesystem::path& file) {
    std::ifstream in(file);
    if (!in) return tl::unexpected(std::format("cannot read {}", file.string()));
    std::stringstream ss;
    ss << in.rdbuf();
    return parse_cluster(ss.str()).map_error([&](std::string e) { return std::format("{}: {}", file.string(), e); });
}

}  // namespace raftkv::net
