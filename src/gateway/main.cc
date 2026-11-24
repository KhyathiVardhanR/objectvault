// ov-gateway: S3-style REST front end. Splits objects into chunks, places
// each chunk on R storage nodes via consistent hashing, stores metadata in
// PostgreSQL (cached in Redis), and runs the replica repair manager.
//
// API
//   GET    /                                   list buckets
//   PUT    /{bucket}                           create bucket
//   DELETE /{bucket}                           delete (empty) bucket
//   GET    /{bucket}?prefix=&start-after=&max-keys=   list objects
//   PUT    /{bucket}/{key}                     upload object
//   GET    /{bucket}/{key}                     download object
//   HEAD   /{bucket}/{key}                     object metadata
//   DELETE /{bucket}/{key}                     delete object
//   POST   /{bucket}/{key}?uploads             start multipart upload
//   PUT    /{bucket}/{key}?uploadId=&partNumber=   upload part
//   POST   /{bucket}/{key}?uploadId=           complete (JSON body lists parts)
//   DELETE /{bucket}/{key}?uploadId=           abort
//   GET    /_admin/status, POST /_admin/cache/flush, GET /healthz

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <iostream>
#include <map>
#include <nlohmann/json.hpp>
#include <regex>
#include <sstream>

#include "common/util.h"
#include "gateway/cluster.h"
#include "gateway/http_server.h"
#include "gateway/metadata.h"
#include "gateway/repair.h"

namespace ov {

using json = nlohmann::json;

struct GatewayMetrics {
  std::shared_ptr<prometheus::Registry> reg = std::make_shared<prometheus::Registry>();
  prometheus::Family<prometheus::Counter>& requests =
      prometheus::BuildCounter().Name("ov_http_requests_total").Help("HTTP requests").Register(*reg);
  prometheus::Family<prometheus::Histogram>& latency =
      prometheus::BuildHistogram().Name("ov_http_request_seconds").Help("HTTP request latency").Register(*reg);
  prometheus::Family<prometheus::Counter>& bytes =
      prometheus::BuildCounter().Name("ov_http_bytes_total").Help("Object bytes in/out").Register(*reg);
  prometheus::Family<prometheus::Histogram>& meta =
      prometheus::BuildHistogram().Name("ov_metadata_lookup_seconds").Help("Object metadata lookup latency by source")
          .Register(*reg);
  prometheus::Family<prometheus::Counter>& cache =
      prometheus::BuildCounter().Name("ov_metadata_cache_total").Help("Metadata cache hits/misses").Register(*reg);
  prometheus::Family<prometheus::Gauge>& nodes =
      prometheus::BuildGauge().Name("ov_storage_node_up").Help("Storage node health").Register(*reg);
  prometheus::Family<prometheus::Counter>& repl =
      prometheus::BuildCounter().Name("ov_replication_total").Help("Replication and repair events").Register(*reg);
  prometheus::Family<prometheus::Gauge>& health =
      prometheus::BuildGauge().Name("ov_replication_health").Help("Replication health").Register(*reg);

  prometheus::Histogram::BucketBoundaries lat_buckets{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05,
                                                      .1,    .25,    .5,    1,    2.5,   5,    10};
  prometheus::Counter& bytes_in = bytes.Add({{"dir", "in"}});
  prometheus::Counter& bytes_out = bytes.Add({{"dir", "out"}});
  prometheus::Histogram& meta_cache = meta.Add({{"source", "cache"}}, lat_buckets);
  prometheus::Histogram& meta_db = meta.Add({{"source", "database"}}, lat_buckets);
  prometheus::Counter& cache_hit = cache.Add({{"result", "hit"}});
  prometheus::Counter& cache_miss = cache.Add({{"result", "miss"}});

  void observe(const std::string& op, int code, double secs) {
    requests.Add({{"op", op}, {"code", std::to_string(code)}}).Increment();
    latency.Add({{"op", op}}, lat_buckets).Observe(secs);
  }
};

struct Config {
  std::string listen = "0.0.0.0:8080";
  std::string metrics = "0.0.0.0:9200";
  std::vector<NodeSpec> nodes;
  std::string pg = "dbname=objectvault";
  std::string redis_host = "127.0.0.1";
  int redis_port = 6379;
  bool cache = true;
  int cache_ttl = 600;
  size_t replicas = 3;
  size_t write_quorum = 2;
  size_t chunk_size = 4 << 20;
  size_t workers = 128;
  size_t io_threads = 96;
  int heartbeat_ms = 500;
  int down_after = 3;
};

// ---------------------------------------------------------------------------
// small HTTP helpers

std::string url_decode(std::string_view s) {
  std::string out;
  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] == '%' && i + 2 < s.size() && std::isxdigit(s[i + 1]) && std::isxdigit(s[i + 2])) {
      out += static_cast<char>(std::stoi(std::string(s.substr(i + 1, 2)), nullptr, 16));
      i += 2;
    } else if (s[i] == '+') {
      out += ' ';
    } else {
      out += s[i];
    }
  }
  return out;
}

struct Target {
  std::string bucket, key;
  std::map<std::string, std::string> query;
  bool has(const std::string& k) const { return query.count(k) > 0; }
  std::string get(const std::string& k, const std::string& def = "") const {
    auto it = query.find(k);
    return it == query.end() ? def : it->second;
  }
};

Target parse_target(std::string_view t) {
  Target out;
  auto q = t.find('?');
  std::string_view path = t.substr(0, q);
  if (q != std::string_view::npos) {
    std::string_view qs = t.substr(q + 1);
    while (!qs.empty()) {
      auto amp = qs.find('&');
      auto kv = qs.substr(0, amp);
      auto eq = kv.find('=');
      out.query[url_decode(kv.substr(0, eq))] = eq == std::string_view::npos ? "" : url_decode(kv.substr(eq + 1));
      if (amp == std::string_view::npos) break;
      qs = qs.substr(amp + 1);
    }
  }
  if (!path.empty() && path[0] == '/') path.remove_prefix(1);
  auto slash = path.find('/');
  out.bucket = url_decode(path.substr(0, slash));
  if (slash != std::string_view::npos) out.key = url_decode(path.substr(slash + 1));
  return out;
}

bool valid_bucket(const std::string& b) {
  static const std::regex re("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$");
  return std::regex_match(b, re);
}

void reply(Response& res, http::status st, const json& body) {
  res.result(st);
  res.set(http::field::content_type, "application/json");
  res.body() = body.dump();
}

void error(Response& res, http::status st, const std::string& code, const std::string& msg = "") {
  reply(res, st, {{"error", code}, {"message", msg}});
}

Digest digest_from_hex(const std::string& h) {
  Digest d{};
  for (size_t i = 0; i < 32; i++) d[i] = static_cast<unsigned char>(std::stoi(h.substr(2 * i, 2), nullptr, 16));
  return d;
}

// ---------------------------------------------------------------------------

class Gateway {
 public:
  Gateway(const Config& cfg, Cluster& cluster, MetaStore& meta, RepairManager& repair, GatewayMetrics& m)
      : cfg_(cfg), cluster_(cluster), meta_(meta), repair_(repair), m_(m) {}

  void handle(const Request& req, Response& res) {
    auto t0 = std::chrono::steady_clock::now();
    std::string op = "other";
    try {
      route(req, res, op);
    } catch (const std::exception& e) {
      error(res, http::status::internal_server_error, "InternalError", e.what());
    }
    m_.observe(op, res.result_int(), seconds_since(t0));
  }

 private:
  void route(const Request& req, Response& res, std::string& op) {
    Target t = parse_target(req.target());
    auto method = req.method();

    if (t.bucket == "healthz") {
      op = "health";
      return reply(res, http::status::ok, {{"ok", true}});
    }
    if (t.bucket == "_admin") return admin(req, t, res, op);

    if (t.bucket.empty()) {
      op = "list_buckets";
      if (method != http::verb::get) return error(res, http::status::method_not_allowed, "MethodNotAllowed");
      return reply(res, http::status::ok, {{"buckets", meta_.list_buckets()}});
    }
    if (t.key.empty()) {
      switch (method) {
        case http::verb::put:
          op = "create_bucket";
          if (!valid_bucket(t.bucket)) return error(res, http::status::bad_request, "InvalidBucketName");
          if (!meta_.create_bucket(t.bucket)) return error(res, http::status::conflict, "BucketAlreadyExists");
          return reply(res, http::status::ok, {{"bucket", t.bucket}});
        case http::verb::delete_: {
          op = "delete_bucket";
          auto r = meta_.delete_bucket(t.bucket);
          if (r == MetaStore::DeleteResult::NotFound) return error(res, http::status::not_found, "NoSuchBucket");
          if (r == MetaStore::DeleteResult::NotEmpty) return error(res, http::status::conflict, "BucketNotEmpty");
          res.result(http::status::no_content);
          return;
        }
        case http::verb::get:
          op = "list_objects";
          return list_objects(t, res);
        default:
          return error(res, http::status::method_not_allowed, "MethodNotAllowed");
      }
    }

    switch (method) {
      case http::verb::put:
        if (t.has("uploadId")) {
          op = "upload_part";
          return upload_part(req, t, res);
        }
        op = "put_object";
        return put_object(req, t, res);
      case http::verb::get:
        op = "get_object";
        return get_object(t, res, false);
      case http::verb::head:
        op = "head_object";
        return get_object(t, res, true);
      case http::verb::delete_:
        if (t.has("uploadId")) {
          op = "abort_upload";
          return abort_upload(t, res);
        }
        op = "delete_object";
        return delete_object(t, res);
      case http::verb::post:
        if (t.has("uploads")) {
          op = "create_upload";
          return create_upload(req, t, res);
        }
        if (t.has("uploadId")) {
          op = "complete_upload";
          return complete_upload(req, t, res);
        }
        return error(res, http::status::bad_request, "InvalidRequest");
      default:
        return error(res, http::status::method_not_allowed, "MethodNotAllowed");
    }
  }

  // Splits data into chunks and writes each to R replicas (quorum W).
  bool store_chunks(std::string_view data, std::vector<ChunkRef>& out) {
    std::vector<Cluster::PendingChunk> pending;
    for (size_t off = 0; off < data.size(); off += cfg_.chunk_size) {
      auto piece = data.substr(off, cfg_.chunk_size);
      pending.push_back({random_id(), piece, sha256(piece), {}});
    }
    bool ok = cluster_.write_batch(pending);
    for (auto& p : pending) out.push_back({p.id, static_cast<int64_t>(p.data.size()), hex(p.digest), p.acked});
    if (!ok) {
      gc(out);
      out.clear();
    }
    return ok;
  }

  // Chunks that reached quorum but not all R replicas get queued for repair.
  void queue_partial(const std::vector<ChunkRef>& chunks) {
    for (const auto& c : chunks) {
      if (c.nodes.size() < cluster_.replicas()) meta_.enqueue_repair(c.id, "-");
    }
  }

  void gc(const std::vector<ChunkRef>& chunks) {
    for (const auto& c : chunks)
      for (const auto& n : c.nodes) cluster_.submit([this, n, id = c.id] { cluster_.remove(n, id); });
  }

  void put_object(const Request& req, const Target& t, Response& res) {
    const std::string& body = req.body();
    ObjectMeta m;
    m.bucket = t.bucket;
    m.key = t.key;
    m.size = static_cast<int64_t>(body.size());
    m.content_type = std::string(req[http::field::content_type]);
    if (m.content_type.empty()) m.content_type = "application/octet-stream";
    if (!meta_.bucket_exists(t.bucket)) return error(res, http::status::not_found, "NoSuchBucket");
    m.etag = hex(sha256(body));
    if (!store_chunks(body, m.chunks))
      return error(res, http::status::service_unavailable, "InsufficientReplicas", "write quorum not reached");
    gc(meta_.put_object(m));
    queue_partial(m.chunks);
    m_.bytes_in.Increment(static_cast<double>(body.size()));
    res.set(http::field::etag, "\"" + m.etag + "\"");
    reply(res, http::status::ok, {{"bucket", m.bucket}, {"key", m.key}, {"size", m.size}, {"etag", m.etag},
                                  {"chunks", m.chunks.size()}});
  }

  std::optional<ObjectMeta> lookup(const std::string& bucket, const std::string& key) {
    auto t0 = std::chrono::steady_clock::now();
    Source src = Source::Database;
    auto m = meta_.get_object(bucket, key, &src);
    double secs = seconds_since(t0);
    if (src == Source::Cache) {
      m_.meta_cache.Observe(secs);
      m_.cache_hit.Increment();
    } else {
      m_.meta_db.Observe(secs);
      if (meta_.cache_enabled()) m_.cache_miss.Increment();
    }
    return m;
  }

  void get_object(const Target& t, Response& res, bool head_only) {
    auto m = lookup(t.bucket, t.key);
    if (!m) return error(res, http::status::not_found, "NoSuchKey");
    res.set(http::field::etag, "\"" + m->etag + "\"");
    res.set(http::field::content_type, m->content_type);
    res.set(http::field::last_modified, m->created);
    res.set("x-ov-chunks", std::to_string(m->chunks.size()));
    if (head_only) {
      res.content_length(static_cast<uint64_t>(m->size));
      return;
    }
    // Fetch all chunks in parallel; each read verifies the SHA-256 and fails
    // over to another replica, queueing repair for any bad copy.
    std::vector<std::string> parts(m->chunks.size());
    std::vector<std::future<bool>> futs;
    for (size_t i = 0; i < m->chunks.size(); i++) {
      futs.push_back(cluster_.submit([this, &m, &parts, i] {
        const auto& c = m->chunks[i];
        return cluster_.read_verified(c.id, digest_from_hex(c.sha256_hex), c.nodes, &parts[i],
                                      [this, &c](const std::string& bad) { meta_.enqueue_repair(c.id, bad); });
      }));
    }
    bool ok = true;
    for (auto& f : futs) ok = f.get() && ok;
    if (!ok) return error(res, http::status::service_unavailable, "ChunkUnavailable", "no healthy replica");
    std::string body;
    body.reserve(static_cast<size_t>(m->size));
    for (auto& p : parts) body += p;
    m_.bytes_out.Increment(static_cast<double>(body.size()));
    res.result(http::status::ok);
    res.body() = std::move(body);
  }

  void delete_object(const Target& t, Response& res) {
    auto chunks = meta_.delete_object(t.bucket, t.key);
    if (!chunks) return error(res, http::status::not_found, "NoSuchKey");
    gc(*chunks);
    res.result(http::status::no_content);
  }

  void list_objects(const Target& t, Response& res) {
    if (!meta_.bucket_exists(t.bucket)) return error(res, http::status::not_found, "NoSuchBucket");
    int limit = std::clamp(std::stoi(t.get("max-keys", "1000")), 1, 1000);
    auto objs = meta_.list_objects(t.bucket, t.get("prefix"), t.get("start-after"), limit + 1);
    bool truncated = static_cast<int>(objs.size()) > limit;
    if (truncated) objs.pop_back();
    json arr = json::array();
    for (auto& o : objs) arr.push_back({{"key", o.key}, {"size", o.size}, {"etag", o.etag}, {"lastModified", o.created}});
    json out{{"bucket", t.bucket}, {"prefix", t.get("prefix")}, {"objects", arr}, {"isTruncated", truncated}};
    if (truncated) out["nextStartAfter"] = objs.back().key;
    reply(res, http::status::ok, out);
  }

  void create_upload(const Request& req, const Target& t, Response& res) {
    if (!meta_.bucket_exists(t.bucket)) return error(res, http::status::not_found, "NoSuchBucket");
    std::string ct(req[http::field::content_type]);
    auto id = meta_.create_upload(t.bucket, t.key, ct.empty() ? "application/octet-stream" : ct);
    reply(res, http::status::ok, {{"bucket", t.bucket}, {"key", t.key}, {"uploadId", id}});
  }

  void upload_part(const Request& req, const Target& t, Response& res) {
    auto up = meta_.get_upload(t.get("uploadId"));
    if (!up || up->bucket != t.bucket || up->key != t.key) return error(res, http::status::not_found, "NoSuchUpload");
    int part = std::atoi(t.get("partNumber").c_str());
    if (part < 1 || part > 10000) return error(res, http::status::bad_request, "InvalidPartNumber");
    const std::string& body = req.body();
    std::vector<ChunkRef> chunks;
    if (!store_chunks(body, chunks))
      return error(res, http::status::service_unavailable, "InsufficientReplicas", "write quorum not reached");
    std::string etag = hex(sha256(body));
    gc(meta_.put_part(t.get("uploadId"), part, static_cast<int64_t>(body.size()), etag, chunks));
    queue_partial(chunks);
    m_.bytes_in.Increment(static_cast<double>(body.size()));
    res.set(http::field::etag, "\"" + etag + "\"");
    reply(res, http::status::ok, {{"partNumber", part}, {"etag", etag}, {"size", body.size()}});
  }

  void complete_upload(const Request& req, const Target& t, Response& res) {
    const std::string uid = t.get("uploadId");
    auto up = meta_.get_upload(uid);
    if (!up || up->bucket != t.bucket || up->key != t.key) return error(res, http::status::not_found, "NoSuchUpload");
    auto body = json::parse(req.body(), nullptr, false);
    if (body.is_discarded() || !body.contains("parts")) return error(res, http::status::bad_request, "MalformedJSON");
    auto stored = meta_.get_parts(uid);
    std::map<int, const MetaStore::Part*> by_num;
    for (auto& p : stored) by_num[p.number] = &p;

    ObjectMeta m{t.bucket, t.key, 0, "", up->content_type, "", {}};
    std::string etag_concat;
    int last = 0;
    for (auto& p : body["parts"]) {
      int n = p.value("partNumber", 0);
      std::string etag = p.value("etag", "");
      if (!etag.empty() && etag.front() == '"') etag = etag.substr(1, etag.size() - 2);
      if (n <= last) return error(res, http::status::bad_request, "InvalidPartOrder");
      last = n;
      auto it = by_num.find(n);
      if (it == by_num.end() || it->second->etag != etag) return error(res, http::status::bad_request, "InvalidPart");
      m.size += it->second->size;
      etag_concat += it->second->etag;
      for (auto& c : it->second->chunks) m.chunks.push_back(c);
    }
    if (last == 0) return error(res, http::status::bad_request, "InvalidPart", "no parts");
    m.etag = hex(sha256(etag_concat)) + "-" + std::to_string(body["parts"].size());
    gc(meta_.put_object(m));      // takes ownership of the listed parts' chunks
    gc(meta_.abort_upload(uid));  // unlisted parts are garbage
    res.set(http::field::etag, "\"" + m.etag + "\"");
    reply(res, http::status::ok, {{"bucket", m.bucket}, {"key", m.key}, {"size", m.size}, {"etag", m.etag}});
  }

  void abort_upload(const Target& t, Response& res) {
    auto up = meta_.get_upload(t.get("uploadId"));
    if (!up) return error(res, http::status::not_found, "NoSuchUpload");
    gc(meta_.abort_upload(t.get("uploadId")));
    res.result(http::status::no_content);
  }

  void admin(const Request& req, const Target& t, Response& res, std::string& op) {
    op = "admin";
    if (t.key == "cache/flush" && req.method() == http::verb::post) {
      meta_.flush_cache();
      return reply(res, http::status::ok, {{"flushed", true}});
    }
    if (t.key == "status") {
      json nodes = json::object();
      for (auto& n : cluster_.node_ids()) nodes[n] = cluster_.is_up(n) ? "up" : "down";
      return reply(res, http::status::ok,
                   {{"nodes", nodes},
                    {"replicas", cluster_.replicas()},
                    {"underReplicated", repair_.under_replicated()},
                    {"lastRecoverySeconds", repair_.last_recovery_seconds()},
                    {"cache", meta_.cache_enabled()}});
    }
    error(res, http::status::not_found, "NoSuchAdminEndpoint");
  }

  const Config& cfg_;
  Cluster& cluster_;
  MetaStore& meta_;
  RepairManager& repair_;
  GatewayMetrics& m_;
};

}  // namespace ov

// ---------------------------------------------------------------------------

static size_t parse_size(const std::string& s) {
  size_t n = std::stoull(s);
  char u = s.empty() ? 0 : static_cast<char>(std::toupper(s.back()));
  if (u == 'K') n <<= 10;
  if (u == 'M') n <<= 20;
  return n;
}

int main(int argc, char** argv) {
  ov::Config cfg;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--listen") cfg.listen = next();
    else if (a == "--metrics") cfg.metrics = next();
    else if (a == "--pg") cfg.pg = next();
    else if (a == "--redis") {
      auto r = next();
      auto c = r.find(':');
      cfg.redis_host = r.substr(0, c);
      cfg.redis_port = std::stoi(r.substr(c + 1));
    } else if (a == "--no-cache") cfg.cache = false;
    else if (a == "--replicas") cfg.replicas = std::stoul(next());
    else if (a == "--write-quorum") cfg.write_quorum = std::stoul(next());
    else if (a == "--chunk-size") cfg.chunk_size = parse_size(next());
    else if (a == "--workers") cfg.workers = std::stoul(next());
    else if (a == "--io-threads") cfg.io_threads = std::stoul(next());
    else if (a == "--heartbeat-ms") cfg.heartbeat_ms = std::stoi(next());
    else if (a == "--nodes") {
      std::stringstream ss(next());
      std::string item;
      while (std::getline(ss, item, ',')) {
        auto eq = item.find('=');
        cfg.nodes.push_back({item.substr(0, eq), item.substr(eq + 1)});
      }
    } else {
      std::cerr << "usage: ov-gateway --nodes s1=host:port,... [--listen H:P] [--metrics H:P] [--pg CONNINFO]\n"
                   "       [--redis H:P] [--no-cache] [--replicas N] [--write-quorum N] [--chunk-size 4M]\n";
      return 2;
    }
  }
  if (cfg.nodes.size() < cfg.replicas) {
    std::cerr << "need at least --replicas storage nodes\n";
    return 2;
  }

  ov::GatewayMetrics gm;
  prometheus::Exposer exposer{cfg.metrics};
  exposer.RegisterCollectable(gm.reg);

  ov::MetaStore meta(cfg.pg, cfg.redis_host, cfg.redis_port, cfg.cache, cfg.cache_ttl);
  meta.init_schema();
  ov::Cluster cluster(cfg.nodes, cfg.replicas, cfg.write_quorum, cfg.io_threads);

  ov::RepairMetrics rm{
      gm.repl.Add({{"event", "chunk_repaired"}}), gm.repl.Add({{"event", "bytes_repaired"}}),
      gm.repl.Add({{"event", "chunk_unrecoverable"}}), gm.health.Add({{"kind", "under_replicated_chunks"}}),
      gm.health.Add({{"kind", "last_recovery_seconds"}})};
  ov::RepairManager repair(cluster, meta, rm);
  for (auto& n : cfg.nodes) gm.nodes.Add({{"node", n.id}}).Set(1);
  cluster.start_heartbeats(std::chrono::milliseconds(cfg.heartbeat_ms), cfg.down_after,
                           [&](const std::string& node, bool up) {
                             gm.nodes.Add({{"node", node}}).Set(up ? 1 : 0);
                             repair.node_changed(node, up);
                           },
                           [&](const std::string& node, const std::string& chunk) {
                             // Scrubber found a corrupt replica: rewrite it from a good copy.
                             meta.enqueue_repair(chunk, node);
                           });
  repair.start();

  ov::Gateway gw(cfg, cluster, meta, repair, gm);
  auto colon = cfg.listen.rfind(':');
  ov::HttpServer server(cfg.listen.substr(0, colon), static_cast<unsigned short>(std::stoi(cfg.listen.substr(colon + 1))),
                        cfg.workers, 5ull << 30, [&](const ov::Request& q, ov::Response& r) { gw.handle(q, r); });
  std::cerr << "gateway listening on " << cfg.listen << " (" << cfg.nodes.size() << " storage nodes, R="
            << cfg.replicas << ", W=" << cfg.write_quorum << ", chunk=" << (cfg.chunk_size >> 10) << "KiB, cache="
            << (cfg.cache ? "redis" : "off") << "), metrics on " << cfg.metrics << "\n";
  server.run();
}
