#pragma once

// Metadata service: PostgreSQL is the source of truth for buckets, objects,
// chunk lists, replica locations and multipart uploads; Redis is a
// cache-aside layer for object metadata (the hot path of every GET/HEAD).

#include <hiredis/hiredis.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <pqxx/pqxx>
#include <string>
#include <vector>

namespace ov {

struct ChunkRef {
  std::string id;
  int64_t size = 0;
  std::string sha256_hex;
  std::vector<std::string> nodes;
};

struct ObjectMeta {
  std::string bucket, key;
  int64_t size = 0;
  std::string etag, content_type, created;
  std::vector<ChunkRef> chunks;
};

struct ListedObject {
  std::string key;
  int64_t size;
  std::string etag, created;
};

enum class Source { Cache, Database };

struct ReplicaTask {
  std::string chunk_id;
  int64_t size;
  std::string sha256_hex;
  std::vector<std::string> nodes;
};

class MetaStore {
 public:
  MetaStore(std::string pg_conninfo, std::string redis_host, int redis_port, bool cache_enabled,
            int cache_ttl_seconds);

  void init_schema();

  // Buckets
  bool create_bucket(const std::string& name);               // false if it exists
  enum class DeleteResult { Ok, NotFound, NotEmpty };
  DeleteResult delete_bucket(const std::string& name);
  std::vector<std::string> list_buckets();
  bool bucket_exists(const std::string& name);

  // Objects. put/delete return the chunks of any replaced object for GC.
  std::vector<ChunkRef> put_object(const ObjectMeta& m);
  std::optional<ObjectMeta> get_object(const std::string& bucket, const std::string& key, Source* src = nullptr);
  std::optional<std::vector<ChunkRef>> delete_object(const std::string& bucket, const std::string& key);
  std::vector<ListedObject> list_objects(const std::string& bucket, const std::string& prefix,
                                         const std::string& start_after, int limit);

  // Multipart uploads
  std::string create_upload(const std::string& bucket, const std::string& key, const std::string& content_type);
  struct UploadInfo {
    std::string bucket, key, content_type;
  };
  std::optional<UploadInfo> get_upload(const std::string& upload_id);
  std::vector<ChunkRef> put_part(const std::string& upload_id, int part, int64_t size, const std::string& etag,
                                 const std::vector<ChunkRef>& chunks);
  struct Part {
    int number;
    int64_t size;
    std::string etag;
    std::vector<ChunkRef> chunks;
  };
  std::vector<Part> get_parts(const std::string& upload_id);
  void finish_upload(const std::string& upload_id);  // drops upload rows (chunks now owned by object)
  std::vector<ChunkRef> abort_upload(const std::string& upload_id);

  // Replica management (repair)
  std::vector<ReplicaTask> chunks_on_node(const std::string& node, int limit);
  std::vector<ReplicaTask> take_repair_queue(int limit);
  void enqueue_repair(const std::string& chunk_id, const std::string& node);
  void move_replica(const std::string& chunk_id, const std::string& from, const std::string& to);
  void drop_replica(const std::string& chunk_id, const std::string& node);
  int64_t count_replicas_on(const std::string& node);
  int64_t repair_queue_size();
  void invalidate_chunk(const std::string& chunk_id);

  // Cache control (admin/benchmarks)
  void flush_cache();
  bool cache_enabled() const { return cache_enabled_; }

 private:
  pqxx::connection& pg();
  redisContext* redis();
  void cache_put(const std::string& ck, const ObjectMeta& m, const std::string& inv_version);
  // Returns the cached value, and in *inv_version the key's invalidation
  // counter as observed in the same round trip (used to make fills safe).
  std::optional<ObjectMeta> cache_get(const std::string& ck, std::string* inv_version);
  void cache_del(const std::string& ck);
  static std::string cache_key(const std::string& bucket, const std::string& key) {
    return "ov:meta:" + bucket + "/" + key;
  }
  std::vector<ChunkRef> load_chunks(pqxx::work& tx, int64_t object_id);

  void filler_loop();

  // Cache fills run on a background thread so a miss costs the caller only
  // the Redis GET plus the database query, not the SET.
  std::mutex fill_mu_;
  std::condition_variable fill_cv_;
  struct Fill {
    std::string key;
    ObjectMeta meta;
    std::string inv_version;
  };
  std::deque<Fill> fills_;
  std::thread filler_;

  std::string conninfo_;
  std::string redis_host_;
  int redis_port_;
  bool cache_enabled_;
  int ttl_;
};

}  // namespace ov
