// ov-bench: load generator for ObjectVault.
//
//   ov-bench upload   --size 8M --concurrency 16 --duration 10      aggregate upload MB/s
//   ov-bench download --size 8M --concurrency 16 --duration 10      aggregate download MB/s
//   ov-bench populate --objects 20000 --concurrency 32               tiny objects for metadata tests
//   ov-bench head     --objects 20000 --hit-rate 0.5 --requests 10000 --concurrency 16
//
// `head` controls the metadata cache hit rate exactly: it flushes the cache,
// warms a fraction of keys, then each request goes to a warm key with
// probability hit-rate and otherwise to a never-read key (a guaranteed miss).

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace beast = boost::beast;
namespace http = beast::http;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using Clock = std::chrono::steady_clock;

struct Opts {
  std::string mode, host = "127.0.0.1", port = "8080", bucket = "bench";
  size_t size = 1 << 20;
  int concurrency = 16;
  double duration = 10;
  int objects = 20000;
  int requests = 10000;
  double hit_rate = 0;
  bool json = false;
  bool size_set = false;
};

class Conn {
 public:
  Conn(const Opts& o) : o_(o), stream_(io_) { connect(); }
  http::response<http::string_body> send(http::verb v, const std::string& target, const std::string* body = nullptr) {
    for (int attempt = 0;; attempt++) {
      try {
        http::request<http::string_body> req{v, target, 11};
        req.set(http::field::host, o_.host);
        req.keep_alive(true);
        if (body) req.body() = *body;
        req.prepare_payload();
        http::write(stream_, req);
        http::response_parser<http::string_body> p;
        p.body_limit(1ull << 32);
        if (v == http::verb::head) p.skip(true);
        http::read(stream_, buf_, p);
        return p.release();
      } catch (const std::exception&) {
        if (attempt > 2) throw;
        connect();
      }
    }
  }

 private:
  void connect() {
    beast::error_code ec;
    stream_.socket().close(ec);
    tcp::resolver r(io_);
    stream_.connect(r.resolve(o_.host, o_.port));
    stream_.socket().set_option(tcp::no_delay(true));
  }
  const Opts& o_;
  asio::io_context io_;
  beast::tcp_stream stream_;
  beast::flat_buffer buf_;
};

struct Stats {
  std::mutex mu;
  std::vector<double> lat;
  std::atomic<uint64_t> ops{0}, bytes{0}, errors{0};
  void add(double s, size_t b) {
    ops++;
    bytes += b;
    std::lock_guard<std::mutex> g(mu);
    lat.push_back(s);
  }
};

double pct(std::vector<double>& v, double p) {
  if (v.empty()) return 0;
  std::sort(v.begin(), v.end());
  return v[static_cast<size_t>(p * (v.size() - 1))];
}
double mean(const std::vector<double>& v) {
  double s = 0;
  for (double x : v) s += x;
  return v.empty() ? 0 : s / v.size();
}

void report(const Opts& o, Stats& st, double elapsed) {
  double mbps = st.bytes / elapsed / 1e6;
  double m = mean(st.lat) * 1e3, p50 = pct(st.lat, .5) * 1e3, p95 = pct(st.lat, .95) * 1e3, p99 = pct(st.lat, .99) * 1e3;
  if (o.json) {
    std::printf("{\"mode\":\"%s\",\"size\":%zu,\"concurrency\":%d,\"hit_rate\":%.2f,\"ops\":%llu,\"errors\":%llu,"
                "\"seconds\":%.3f,\"MBps\":%.2f,\"ops_per_sec\":%.1f,\"mean_ms\":%.4f,\"p50_ms\":%.4f,\"p95_ms\":%.4f,"
                "\"p99_ms\":%.4f}\n",
                o.mode.c_str(), o.size, o.concurrency, o.hit_rate, (unsigned long long)st.ops.load(),
                (unsigned long long)st.errors.load(), elapsed, mbps, st.ops / elapsed, m, p50, p95, p99);
  } else {
    std::printf("%-9s size=%-9zu conc=%-3d ops=%-7llu err=%-3llu %8.1f MB/s %9.1f ops/s  mean=%.3fms p50=%.3fms "
                "p95=%.3fms p99=%.3fms\n",
                o.mode.c_str(), o.size, o.concurrency, (unsigned long long)st.ops.load(),
                (unsigned long long)st.errors.load(), mbps, st.ops / elapsed, m, p50, p95, p99);
  }
}

void ensure_bucket(const Opts& o) {
  Conn c(o);
  c.send(http::verb::put, "/" + o.bucket);
}

// Timed closed-loop run: each worker repeats op(conn, worker, iteration).
template <class F>
void timed(const Opts& o, Stats& st, F op) {
  std::atomic<bool> stop{false};
  std::vector<std::thread> ts;
  auto t0 = Clock::now();
  for (int w = 0; w < o.concurrency; w++) {
    ts.emplace_back([&, w] {
      Conn c(o);
      for (uint64_t i = 0; !stop; i++) {
        auto s = Clock::now();
        size_t bytes = 0;
        bool ok = op(c, w, i, bytes);
        double d = std::chrono::duration<double>(Clock::now() - s).count();
        if (ok) st.add(d, bytes);
        else st.errors++;
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::duration<double>(o.duration));
  stop = true;
  for (auto& t : ts) t.join();
  report(o, st, std::chrono::duration<double>(Clock::now() - t0).count());
}

std::string random_body(size_t n) {
  std::string s(n, '\0');
  std::mt19937_64 rng(42);
  for (size_t i = 0; i + 8 <= n; i += 8) {
    uint64_t x = rng();
    std::memcpy(&s[i], &x, 8);
  }
  return s;
}

int main(int argc, char** argv) {
  Opts o;
  if (argc < 2) {
    std::cerr << "usage: ov-bench {upload|download|populate|head} [--size N[K|M]] [--concurrency N] [--duration S]\n"
                 "                [--objects N] [--requests N] [--hit-rate F] [--bucket B] [--host H] [--port P] [--json]\n";
    return 2;
  }
  o.mode = argv[1];
  for (int i = 2; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--size") {
      std::string s = next();
      size_t n = std::stoull(s);
      char u = static_cast<char>(std::toupper(s.back()));
      o.size = u == 'K' ? n << 10 : u == 'M' ? n << 20 : n;
      o.size_set = true;
    } else if (a == "--concurrency") o.concurrency = std::stoi(next());
    else if (a == "--duration") o.duration = std::stod(next());
    else if (a == "--objects") o.objects = std::stoi(next());
    else if (a == "--requests") o.requests = std::stoi(next());
    else if (a == "--hit-rate") o.hit_rate = std::stod(next());
    else if (a == "--bucket") o.bucket = next();
    else if (a == "--host") o.host = next();
    else if (a == "--port") o.port = next();
    else if (a == "--json") o.json = true;
  }
  ensure_bucket(o);
  Stats st;

  if (o.mode == "upload") {
    const std::string body = random_body(o.size);
    timed(o, st, [&](Conn& c, int w, uint64_t i, size_t& bytes) {
      auto r = c.send(http::verb::put, "/" + o.bucket + "/up/" + std::to_string(w) + "/" + std::to_string(i % 64), &body);
      bytes = body.size();
      return r.result() == http::status::ok;
    });
  } else if (o.mode == "download") {
    const std::string body = random_body(o.size);
    {
      Conn c(o);
      for (int k = 0; k < 16; k++) c.send(http::verb::put, "/" + o.bucket + "/down/" + std::to_string(k), &body);
    }
    timed(o, st, [&](Conn& c, int w, uint64_t i, size_t& bytes) {
      auto r = c.send(http::verb::get, "/" + o.bucket + "/down/" + std::to_string((w + i) % 16));
      bytes = r.body().size();
      return r.result() == http::status::ok && bytes == o.size;
    });
  } else if (o.mode == "populate") {
    const std::string body = o.size_set ? random_body(o.size) : std::string("x");
    std::atomic<int> next{0};
    std::vector<std::thread> ts;
    auto t0 = Clock::now();
    for (int w = 0; w < o.concurrency; w++)
      ts.emplace_back([&] {
        Conn c(o);
        for (int k; (k = next++) < o.objects;) {
          auto s = Clock::now();
          auto r = c.send(http::verb::put, "/" + o.bucket + "/meta/" + std::to_string(k), &body);
          if (r.result() == http::status::ok) st.add(std::chrono::duration<double>(Clock::now() - s).count(), 1);
          else st.errors++;
        }
      });
    for (auto& t : ts) t.join();
    report(o, st, std::chrono::duration<double>(Clock::now() - t0).count());
  } else if (o.mode == "head") {
    {
      Conn c(o);
      c.send(http::verb::post, "/_admin/cache/flush");
    }
    int warm = static_cast<int>(o.objects * o.hit_rate);
    if (o.hit_rate > 0 && warm == 0) warm = 1;
    // Warm the cache for keys [0, warm).
    {
      std::atomic<int> next{0};
      std::vector<std::thread> ts;
      for (int w = 0; w < o.concurrency; w++)
        ts.emplace_back([&] {
          Conn c(o);
          for (int k; (k = next++) < warm;) c.send(http::verb::head, "/" + o.bucket + "/meta/" + std::to_string(k));
        });
      for (auto& t : ts) t.join();
    }
    int cold_available = o.objects - warm;
    std::atomic<int> cold_next{0}, issued{0};
    std::atomic<int> hits{0};
    std::vector<std::thread> ts;
    auto t0 = Clock::now();
    for (int w = 0; w < o.concurrency; w++)
      ts.emplace_back([&, w] {
        Conn c(o);
        std::mt19937 rng(1000 + w);
        std::uniform_real_distribution<double> u(0, 1);
        while (issued++ < o.requests) {
          int k;
          if (warm > 0 && (u(rng) < o.hit_rate || cold_next >= cold_available)) {
            k = static_cast<int>(rng() % warm);
            hits++;
          } else {
            k = warm + cold_next++;
          }
          auto s = Clock::now();
          auto r = c.send(http::verb::head, "/" + o.bucket + "/meta/" + std::to_string(k));
          if (r.result() == http::status::ok) st.add(std::chrono::duration<double>(Clock::now() - s).count(), 0);
          else st.errors++;
        }
      });
    for (auto& t : ts) t.join();
    o.hit_rate = static_cast<double>(hits) / std::max<uint64_t>(1, st.ops + st.errors);
    report(o, st, std::chrono::duration<double>(Clock::now() - t0).count());
  } else {
    std::cerr << "unknown mode " << o.mode << "\n";
    return 2;
  }
}
