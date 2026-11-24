#pragma once

#include <openssl/evp.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <string_view>

namespace ov {

using Digest = std::array<unsigned char, 32>;

inline Digest sha256(std::string_view data) {
  Digest d{};
  unsigned int len = 0;
  EVP_Digest(data.data(), data.size(), d.data(), &len, EVP_sha256(), nullptr);
  return d;
}

// Incremental SHA-256 for whole-object ETags.
class Sha256Stream {
 public:
  Sha256Stream() : ctx_(EVP_MD_CTX_new()) { EVP_DigestInit_ex(ctx_, EVP_sha256(), nullptr); }
  ~Sha256Stream() { EVP_MD_CTX_free(ctx_); }
  Sha256Stream(const Sha256Stream&) = delete;
  Sha256Stream& operator=(const Sha256Stream&) = delete;
  void update(std::string_view s) { EVP_DigestUpdate(ctx_, s.data(), s.size()); }
  Digest finish() {
    Digest d{};
    unsigned int len = 0;
    EVP_DigestFinal_ex(ctx_, d.data(), &len);
    return d;
  }

 private:
  EVP_MD_CTX* ctx_;
};

inline std::string hex(const unsigned char* p, size_t n) {
  static const char* k = "0123456789abcdef";
  std::string out(n * 2, '0');
  for (size_t i = 0; i < n; i++) {
    out[2 * i] = k[p[i] >> 4];
    out[2 * i + 1] = k[p[i] & 15];
  }
  return out;
}
inline std::string hex(const Digest& d) { return hex(d.data(), d.size()); }
inline std::string hex(std::string_view s) {
  return hex(reinterpret_cast<const unsigned char*>(s.data()), s.size());
}

inline std::string digest_bytes(const Digest& d) {
  return std::string(reinterpret_cast<const char*>(d.data()), d.size());
}

// 128-bit random identifier, hex encoded.
inline std::string random_id() {
  thread_local std::mt19937_64 rng{std::random_device{}() ^
                                   static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};
  uint64_t a = rng(), b = rng();
  char buf[33];
  std::snprintf(buf, sizeof buf, "%016llx%016llx", static_cast<unsigned long long>(a),
                static_cast<unsigned long long>(b));
  return buf;
}

inline double seconds_since(std::chrono::steady_clock::time_point t) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - t).count();
}

}  // namespace ov
