#pragma once

// Consistent-hash ring with virtual nodes. Placement for a chunk is the first
// R distinct nodes clockwise from hash(chunk_id), skipping excluded (down)
// nodes, so adding or losing a node only moves the chunks adjacent to it.

#include <openssl/evp.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <set>
#include <string>
#include <vector>

namespace ov {

inline uint64_t hash64(const std::string& s) {
  unsigned char d[32];
  unsigned int len = 0;
  EVP_Digest(s.data(), s.size(), d, &len, EVP_sha256(), nullptr);
  uint64_t h;
  std::memcpy(&h, d, sizeof h);
  return h;
}

class HashRing {
 public:
  HashRing(const std::vector<std::string>& nodes, int vnodes = 128) {
    for (const auto& n : nodes) {
      for (int v = 0; v < vnodes; v++) points_.push_back({hash64(n + "#" + std::to_string(v)), n});
    }
    std::sort(points_.begin(), points_.end());
    nodes_ = nodes;
  }

  std::vector<std::string> place(const std::string& key, size_t replicas,
                                 const std::set<std::string>& exclude = {}) const {
    std::vector<std::string> out;
    if (points_.empty()) return out;
    uint64_t h = hash64(key);
    auto it = std::lower_bound(points_.begin(), points_.end(), Point{h, ""});
    for (size_t i = 0; i < points_.size() && out.size() < replicas; i++, it++) {
      if (it == points_.end()) it = points_.begin();
      const auto& n = it->node;
      if (exclude.count(n) || std::find(out.begin(), out.end(), n) != out.end()) continue;
      out.push_back(n);
    }
    return out;
  }

  const std::vector<std::string>& nodes() const { return nodes_; }

 private:
  struct Point {
    uint64_t h;
    std::string node;
    bool operator<(const Point& o) const { return h < o.h; }
  };
  std::vector<Point> points_;
  std::vector<std::string> nodes_;
};

}  // namespace ov
