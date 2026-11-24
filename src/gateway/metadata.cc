#include "gateway/metadata.h"

#include "common/util.h"

#include <atomic>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>

namespace ov {

using json = nlohmann::json;

namespace {
std::atomic<uint64_t> g_cache_generation{1};

std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ','))
    if (!item.empty()) out.push_back(item);
  return out;
}

const char* kSchema = R"SQL(
CREATE TABLE IF NOT EXISTS buckets(
  id BIGSERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS objects(
  id BIGSERIAL PRIMARY KEY,
  bucket_id BIGINT NOT NULL REFERENCES buckets(id),
  key TEXT NOT NULL,
  size BIGINT NOT NULL,
  etag TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(bucket_id, key));
-- prefix listing: WHERE bucket_id = $1 AND key LIKE 'prefix%' ORDER BY key
CREATE INDEX IF NOT EXISTS objects_prefix_idx ON objects(bucket_id, key text_pattern_ops);
CREATE TABLE IF NOT EXISTS uploads(
  upload_id TEXT PRIMARY KEY,
  bucket_id BIGINT NOT NULL REFERENCES buckets(id),
  key TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS parts(
  upload_id TEXT NOT NULL REFERENCES uploads(upload_id) ON DELETE CASCADE,
  part_number INT NOT NULL,
  size BIGINT NOT NULL,
  etag TEXT NOT NULL,
  PRIMARY KEY(upload_id, part_number));
-- A chunk belongs to an object, or (before completion) to a multipart part.
CREATE TABLE IF NOT EXISTS chunks(
  chunk_id TEXT PRIMARY KEY,
  object_id BIGINT REFERENCES objects(id) ON DELETE CASCADE,
  upload_id TEXT,
  part_number INT,
  seq INT NOT NULL,
  size BIGINT NOT NULL,
  sha256 TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS chunks_object_idx ON chunks(object_id, seq);
CREATE INDEX IF NOT EXISTS chunks_upload_idx ON chunks(upload_id, part_number, seq);
CREATE TABLE IF NOT EXISTS replicas(
  chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
  node TEXT NOT NULL,
  PRIMARY KEY(chunk_id, node));
CREATE INDEX IF NOT EXISTS replicas_node_idx ON replicas(node);
CREATE TABLE IF NOT EXISTS repair_queue(
  chunk_id TEXT NOT NULL,
  node TEXT NOT NULL,
  PRIMARY KEY(chunk_id, node));
)SQL";

void prepare_all(pqxx::connection& c) {
  c.prepare("get_object", R"SQL(
    SELECT o.id, o.size, o.etag, o.content_type, o.created_at::text,
           c.chunk_id, c.size, c.sha256, COALESCE(string_agg(r.node, ',' ORDER BY r.node), '')
    FROM buckets b
    JOIN objects o ON o.bucket_id = b.id
    LEFT JOIN chunks c ON c.object_id = o.id
    LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
    WHERE b.name = $1 AND o.key = $2
    GROUP BY o.id, c.chunk_id, c.seq, c.size, c.sha256
    ORDER BY c.seq)SQL");
  c.prepare("bucket_id", "SELECT id FROM buckets WHERE name = $1");
}
}  // namespace

MetaStore::MetaStore(std::string pg_conninfo, std::string redis_host, int redis_port, bool cache_enabled,
                     int cache_ttl_seconds)
    : conninfo_(std::move(pg_conninfo)), redis_host_(std::move(redis_host)), redis_port_(redis_port),
      cache_enabled_(cache_enabled), ttl_(cache_ttl_seconds) {}

pqxx::connection& MetaStore::pg() {
  thread_local std::unique_ptr<pqxx::connection> conn;
  if (!conn || !conn->is_open()) {
    conn = std::make_unique<pqxx::connection>(conninfo_);
    prepare_all(*conn);
  }
  return *conn;
}

redisContext* MetaStore::redis() {
  thread_local redisContext* ctx = nullptr;
  if (ctx && ctx->err) {
    redisFree(ctx);
    ctx = nullptr;
  }
  if (!ctx) {
    timeval tv{0, 200000};
    ctx = redisConnectWithTimeout(redis_host_.c_str(), redis_port_, tv);
    if (ctx && !ctx->err) redisSetTimeout(ctx, tv);
  }
  return ctx;
}

void MetaStore::init_schema() {
  pqxx::work tx(pg());
  tx.exec(kSchema);
  tx.commit();
}

// ---------------------------------------------------------------------------
// buckets

bool MetaStore::create_bucket(const std::string& name) {
  pqxx::work tx(pg());
  auto r = tx.exec("INSERT INTO buckets(name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id", pqxx::params{name});
  tx.commit();
  return !r.empty();
}

bool MetaStore::bucket_exists(const std::string& name) {
  pqxx::work tx(pg());
  return !tx.exec(pqxx::prepped{"bucket_id"}, pqxx::params{name}).empty();
}

MetaStore::DeleteResult MetaStore::delete_bucket(const std::string& name) {
  pqxx::work tx(pg());
  auto b = tx.exec(pqxx::prepped{"bucket_id"}, pqxx::params{name});
  if (b.empty()) return DeleteResult::NotFound;
  auto id = b[0][0].as<int64_t>();
  if (!tx.exec("SELECT 1 FROM objects WHERE bucket_id = $1 LIMIT 1", pqxx::params{id}).empty() ||
      !tx.exec("SELECT 1 FROM uploads WHERE bucket_id = $1 LIMIT 1", pqxx::params{id}).empty())
    return DeleteResult::NotEmpty;
  tx.exec("DELETE FROM buckets WHERE id = $1", pqxx::params{id});
  tx.commit();
  return DeleteResult::Ok;
}

std::vector<std::string> MetaStore::list_buckets() {
  pqxx::work tx(pg());
  std::vector<std::string> out;
  for (auto row : tx.exec("SELECT name FROM buckets ORDER BY name")) out.push_back(row[0].as<std::string>());
  return out;
}

// ---------------------------------------------------------------------------
// objects

std::vector<ChunkRef> MetaStore::load_chunks(pqxx::work& tx, int64_t object_id) {
  std::vector<ChunkRef> out;
  auto r = tx.exec(R"SQL(
    SELECT c.chunk_id, c.size, c.sha256, COALESCE(string_agg(r.node, ','), '')
    FROM chunks c LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
    WHERE c.object_id = $1 GROUP BY c.chunk_id, c.seq, c.size, c.sha256 ORDER BY c.seq)SQL",
                   pqxx::params{object_id});
  for (auto row : r)
    out.push_back({row[0].as<std::string>(), row[1].as<int64_t>(), row[2].as<std::string>(),
                   split_csv(row[3].as<std::string>())});
  return out;
}

std::vector<ChunkRef> MetaStore::put_object(const ObjectMeta& m) {
  std::vector<ChunkRef> replaced;
  {
    pqxx::work tx(pg());
    auto b = tx.exec(pqxx::prepped{"bucket_id"}, pqxx::params{m.bucket});
    if (b.empty()) throw std::runtime_error("no such bucket");
    auto bid = b[0][0].as<int64_t>();
    auto old = tx.exec("SELECT id FROM objects WHERE bucket_id = $1 AND key = $2 FOR UPDATE", pqxx::params{bid, m.key});
    if (!old.empty()) {
      auto oid = old[0][0].as<int64_t>();
      replaced = load_chunks(tx, oid);
      tx.exec("DELETE FROM objects WHERE id = $1", pqxx::params{oid});
    }
    auto oid = tx.exec("INSERT INTO objects(bucket_id, key, size, etag, content_type) VALUES ($1,$2,$3,$4,$5) RETURNING id",
                       pqxx::params{bid, m.key, m.size, m.etag, m.content_type})[0][0]
                   .as<int64_t>();
    int seq = 0;
    for (const auto& c : m.chunks) {
      tx.exec("INSERT INTO chunks(chunk_id, object_id, seq, size, sha256) VALUES ($1,$2,$3,$4,$5) "
              "ON CONFLICT (chunk_id) DO UPDATE SET object_id = EXCLUDED.object_id, upload_id = NULL, "
              "part_number = NULL, seq = EXCLUDED.seq",
              pqxx::params{c.id, oid, seq++, c.size, c.sha256_hex});
      for (const auto& n : c.nodes)
        tx.exec("INSERT INTO replicas(chunk_id, node) VALUES ($1,$2) ON CONFLICT DO NOTHING", pqxx::params{c.id, n});
    }
    tx.commit();
  }
  cache_del(cache_key(m.bucket, m.key));
  return replaced;
}

std::optional<ObjectMeta> MetaStore::get_object(const std::string& bucket, const std::string& key, Source* src) {
  const std::string ck = cache_key(bucket, key);
  if (cache_enabled_) {
    if (auto hit = cache_get(ck)) {
      if (src) *src = Source::Cache;
      return hit;
    }
  }
  if (src) *src = Source::Database;
  pqxx::work tx(pg());
  auto r = tx.exec(pqxx::prepped{"get_object"}, pqxx::params{bucket, key});
  if (r.empty()) return std::nullopt;
  ObjectMeta m;
  m.bucket = bucket;
  m.key = key;
  m.size = r[0][1].as<int64_t>();
  m.etag = r[0][2].as<std::string>();
  m.content_type = r[0][3].as<std::string>();
  m.created = r[0][4].as<std::string>();
  for (auto row : r) {
    if (row[5].is_null()) continue;  // empty object
    m.chunks.push_back({row[5].as<std::string>(), row[6].as<int64_t>(), row[7].as<std::string>(),
                        split_csv(row[8].as<std::string>())});
  }
  if (cache_enabled_) cache_put(ck, m);
  return m;
}

std::optional<std::vector<ChunkRef>> MetaStore::delete_object(const std::string& bucket, const std::string& key) {
  std::vector<ChunkRef> chunks;
  {
    pqxx::work tx(pg());
    auto r = tx.exec("SELECT o.id FROM objects o JOIN buckets b ON b.id = o.bucket_id "
                     "WHERE b.name = $1 AND o.key = $2 FOR UPDATE OF o",
                     pqxx::params{bucket, key});
    if (r.empty()) return std::nullopt;
    auto oid = r[0][0].as<int64_t>();
    chunks = load_chunks(tx, oid);
    tx.exec("DELETE FROM objects WHERE id = $1", pqxx::params{oid});
    tx.commit();
  }
  cache_del(cache_key(bucket, key));
  return chunks;
}

std::vector<ListedObject> MetaStore::list_objects(const std::string& bucket, const std::string& prefix,
                                                  const std::string& start_after, int limit) {
  pqxx::work tx(pg());
  // Escape LIKE metacharacters in the prefix so the text_pattern_ops index is used.
  std::string pat;
  for (char c : prefix) {
    if (c == '%' || c == '_' || c == '\\') pat += '\\';
    pat += c;
  }
  pat += '%';
  auto r = tx.exec(R"SQL(
    SELECT o.key, o.size, o.etag, o.created_at::text FROM objects o JOIN buckets b ON b.id = o.bucket_id
    WHERE b.name = $1 AND o.key LIKE $2 AND o.key > $3 ORDER BY o.key LIMIT $4)SQL",
                   pqxx::params{bucket, pat, start_after, limit});
  std::vector<ListedObject> out;
  for (auto row : r)
    out.push_back({row[0].as<std::string>(), row[1].as<int64_t>(), row[2].as<std::string>(), row[3].as<std::string>()});
  return out;
}

// ---------------------------------------------------------------------------
// multipart

std::string MetaStore::create_upload(const std::string& bucket, const std::string& key,
                                     const std::string& content_type) {
  pqxx::work tx(pg());
  auto b = tx.exec(pqxx::prepped{"bucket_id"}, pqxx::params{bucket});
  if (b.empty()) throw std::runtime_error("no such bucket");
  std::string id = random_id();
  tx.exec("INSERT INTO uploads(upload_id, bucket_id, key, content_type) VALUES ($1,$2,$3,$4)",
          pqxx::params{id, b[0][0].as<int64_t>(), key, content_type});
  tx.commit();
  return id;
}

std::optional<MetaStore::UploadInfo> MetaStore::get_upload(const std::string& upload_id) {
  pqxx::work tx(pg());
  auto r = tx.exec("SELECT b.name, u.key, u.content_type FROM uploads u JOIN buckets b ON b.id = u.bucket_id "
                   "WHERE u.upload_id = $1",
                   pqxx::params{upload_id});
  if (r.empty()) return std::nullopt;
  return UploadInfo{r[0][0].as<std::string>(), r[0][1].as<std::string>(), r[0][2].as<std::string>()};
}

std::vector<ChunkRef> MetaStore::put_part(const std::string& upload_id, int part, int64_t size,
                                          const std::string& etag, const std::vector<ChunkRef>& chunks) {
  pqxx::work tx(pg());
  std::vector<ChunkRef> replaced;
  for (auto row : tx.exec("SELECT chunk_id, size, sha256 FROM chunks WHERE upload_id = $1 AND part_number = $2",
                          pqxx::params{upload_id, part}))
    replaced.push_back({row[0].as<std::string>(), row[1].as<int64_t>(), row[2].as<std::string>(), {}});
  if (!replaced.empty()) {
    for (auto row : tx.exec("SELECT r.chunk_id, r.node FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
                            "WHERE c.upload_id = $1 AND c.part_number = $2",
                            pqxx::params{upload_id, part}))
      for (auto& c : replaced)
        if (c.id == row[0].as<std::string>()) c.nodes.push_back(row[1].as<std::string>());
    tx.exec("DELETE FROM chunks WHERE upload_id = $1 AND part_number = $2", pqxx::params{upload_id, part});
  }
  tx.exec("INSERT INTO parts(upload_id, part_number, size, etag) VALUES ($1,$2,$3,$4) "
          "ON CONFLICT (upload_id, part_number) DO UPDATE SET size = EXCLUDED.size, etag = EXCLUDED.etag",
          pqxx::params{upload_id, part, size, etag});
  int seq = 0;
  for (const auto& c : chunks) {
    tx.exec("INSERT INTO chunks(chunk_id, upload_id, part_number, seq, size, sha256) VALUES ($1,$2,$3,$4,$5,$6)",
            pqxx::params{c.id, upload_id, part, seq++, c.size, c.sha256_hex});
    for (const auto& n : c.nodes)
      tx.exec("INSERT INTO replicas(chunk_id, node) VALUES ($1,$2)", pqxx::params{c.id, n});
  }
  tx.commit();
  return replaced;
}

std::vector<MetaStore::Part> MetaStore::get_parts(const std::string& upload_id) {
  pqxx::work tx(pg());
  std::vector<Part> parts;
  for (auto row : tx.exec("SELECT part_number, size, etag FROM parts WHERE upload_id = $1 ORDER BY part_number",
                          pqxx::params{upload_id}))
    parts.push_back({row[0].as<int>(), row[1].as<int64_t>(), row[2].as<std::string>(), {}});
  auto r = tx.exec(R"SQL(
    SELECT c.part_number, c.chunk_id, c.size, c.sha256, COALESCE(string_agg(r.node, ','), '')
    FROM chunks c LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
    WHERE c.upload_id = $1 GROUP BY c.part_number, c.seq, c.chunk_id, c.size, c.sha256
    ORDER BY c.part_number, c.seq)SQL",
                   pqxx::params{upload_id});
  for (auto row : r) {
    int pn = row[0].as<int>();
    for (auto& p : parts)
      if (p.number == pn)
        p.chunks.push_back({row[1].as<std::string>(), row[2].as<int64_t>(), row[3].as<std::string>(),
                            split_csv(row[4].as<std::string>())});
  }
  return parts;
}

void MetaStore::finish_upload(const std::string& upload_id) {
  pqxx::work tx(pg());
  tx.exec("DELETE FROM uploads WHERE upload_id = $1", pqxx::params{upload_id});
  tx.commit();
}

std::vector<ChunkRef> MetaStore::abort_upload(const std::string& upload_id) {
  std::vector<ChunkRef> chunks;
  for (auto& p : get_parts(upload_id))
    for (auto& c : p.chunks) chunks.push_back(c);
  pqxx::work tx(pg());
  tx.exec("DELETE FROM chunks WHERE upload_id = $1", pqxx::params{upload_id});
  tx.exec("DELETE FROM uploads WHERE upload_id = $1", pqxx::params{upload_id});
  tx.commit();
  return chunks;
}

// ---------------------------------------------------------------------------
// replica management

static std::vector<ReplicaTask> replica_tasks(pqxx::result r) {
  std::vector<ReplicaTask> out;
  for (auto row : r)
    out.push_back({row[0].as<std::string>(), row[1].as<int64_t>(), row[2].as<std::string>(),
                   split_csv(row[3].as<std::string>())});
  return out;
}

std::vector<ReplicaTask> MetaStore::chunks_on_node(const std::string& node, int limit) {
  pqxx::work tx(pg());
  return replica_tasks(tx.exec(R"SQL(
    SELECT c.chunk_id, c.size, c.sha256, string_agg(r2.node, ',')
    FROM (SELECT chunk_id FROM replicas WHERE node = $1 LIMIT $2) x
    JOIN chunks c ON c.chunk_id = x.chunk_id
    JOIN replicas r2 ON r2.chunk_id = c.chunk_id
    GROUP BY c.chunk_id, c.size, c.sha256)SQL",
                               pqxx::params{node, limit}));
}

std::vector<ReplicaTask> MetaStore::take_repair_queue(int limit) {
  pqxx::work tx(pg());
  auto r = tx.exec(R"SQL(
    WITH q AS (DELETE FROM repair_queue WHERE ctid IN (SELECT ctid FROM repair_queue LIMIT $1) RETURNING chunk_id, node)
    SELECT c.chunk_id, c.size, c.sha256, COALESCE(string_agg(r.node, ','), ''), q.node
    FROM q JOIN chunks c ON c.chunk_id = q.chunk_id LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
    GROUP BY c.chunk_id, c.size, c.sha256, q.node)SQL",
                   pqxx::params{limit});
  tx.commit();
  std::vector<ReplicaTask> out;
  for (auto row : r) {
    ReplicaTask t{row[0].as<std::string>(), row[1].as<int64_t>(), row[2].as<std::string>(),
                  split_csv(row[3].as<std::string>())};
    // The node in the queue holds a bad/missing copy: treat it as not holding one.
    auto bad = row[4].as<std::string>();
    std::erase(t.nodes, bad);
    out.push_back(std::move(t));
  }
  return out;
}

void MetaStore::enqueue_repair(const std::string& chunk_id, const std::string& node) {
  pqxx::work tx(pg());
  tx.exec("DELETE FROM replicas WHERE chunk_id = $1 AND node = $2", pqxx::params{chunk_id, node});
  tx.exec("INSERT INTO repair_queue(chunk_id, node) VALUES ($1,$2) ON CONFLICT DO NOTHING",
          pqxx::params{chunk_id, node});
  tx.commit();
  invalidate_chunk(chunk_id);
}

void MetaStore::move_replica(const std::string& chunk_id, const std::string& from, const std::string& to) {
  {
    pqxx::work tx(pg());
    if (!from.empty())
      tx.exec("DELETE FROM replicas WHERE chunk_id = $1 AND node = $2", pqxx::params{chunk_id, from});
    tx.exec("INSERT INTO replicas(chunk_id, node) SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM chunks WHERE chunk_id = $1) "
            "ON CONFLICT DO NOTHING",
            pqxx::params{chunk_id, to});
    tx.commit();
  }
  invalidate_chunk(chunk_id);
}

void MetaStore::drop_replica(const std::string& chunk_id, const std::string& node) {
  {
    pqxx::work tx(pg());
    tx.exec("DELETE FROM replicas WHERE chunk_id = $1 AND node = $2", pqxx::params{chunk_id, node});
    tx.commit();
  }
  invalidate_chunk(chunk_id);
}

int64_t MetaStore::count_replicas_on(const std::string& node) {
  pqxx::work tx(pg());
  return tx.exec("SELECT count(*) FROM replicas WHERE node = $1", pqxx::params{node})[0][0].as<int64_t>();
}

int64_t MetaStore::repair_queue_size() {
  pqxx::work tx(pg());
  return tx.exec("SELECT count(*) FROM repair_queue")[0][0].as<int64_t>();
}

void MetaStore::invalidate_chunk(const std::string& chunk_id) {
  if (!cache_enabled_) return;
  pqxx::work tx(pg());
  auto r = tx.exec("SELECT b.name, o.key FROM chunks c JOIN objects o ON o.id = c.object_id "
                   "JOIN buckets b ON b.id = o.bucket_id WHERE c.chunk_id = $1",
                   pqxx::params{chunk_id});
  for (auto row : r) cache_del(cache_key(row[0].as<std::string>(), row[1].as<std::string>()));
}

// ---------------------------------------------------------------------------
// Redis cache (keys are versioned by a generation so flush is O(1))

static std::string versioned(const std::string& ck) {
  return std::to_string(g_cache_generation.load()) + ":" + ck;
}

void MetaStore::cache_put(const std::string& ck, const ObjectMeta& m) {
  json j{{"s", m.size}, {"e", m.etag}, {"t", m.content_type}, {"c", m.created}};
  auto& arr = j["k"] = json::array();
  for (const auto& c : m.chunks) arr.push_back({c.id, c.size, c.sha256_hex, c.nodes});
  auto payload = j.dump();
  auto key = versioned(ck);
  if (auto* r = redis()) {
    auto* reply = static_cast<redisReply*>(
        redisCommand(r, "SET %b %b EX %d", key.data(), key.size(), payload.data(), payload.size(), ttl_));
    if (reply) freeReplyObject(reply);
  }
}

std::optional<ObjectMeta> MetaStore::cache_get(const std::string& ck) {
  auto* r = redis();
  if (!r || r->err) return std::nullopt;
  auto key = versioned(ck);
  auto* reply = static_cast<redisReply*>(redisCommand(r, "GET %b", key.data(), key.size()));
  if (!reply) return std::nullopt;
  std::optional<ObjectMeta> out;
  if (reply->type == REDIS_REPLY_STRING) {
    auto j = json::parse(std::string(reply->str, reply->len), nullptr, false);
    if (!j.is_discarded()) {
      ObjectMeta m;
      auto slash = ck.find('/', 8);
      m.bucket = ck.substr(8, slash - 8);
      m.key = ck.substr(slash + 1);
      m.size = j["s"];
      m.etag = j["e"];
      m.content_type = j["t"];
      m.created = j["c"];
      for (auto& c : j["k"]) m.chunks.push_back({c[0], c[1], c[2], c[3].get<std::vector<std::string>>()});
      out = std::move(m);
    }
  }
  freeReplyObject(reply);
  return out;
}

void MetaStore::cache_del(const std::string& ck) {
  if (!cache_enabled_) return;
  auto key = versioned(ck);
  if (auto* r = redis()) {
    auto* reply = static_cast<redisReply*>(redisCommand(r, "DEL %b", key.data(), key.size()));
    if (reply) freeReplyObject(reply);
  }
}

void MetaStore::flush_cache() { g_cache_generation++; }

}  // namespace ov
