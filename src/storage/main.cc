// ov-storage: a storage node. Stores immutable chunks on local disk, each file
// prefixed with the chunk's SHA-256 so integrity is verified on every write
// and every read. Writes are atomic (temp file + fsync + rename).

#include <fcntl.h>
#include <grpcpp/grpcpp.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/util.h"
#include "storage/group_sync.h"
#include "storage.grpc.pb.h"

namespace fs = std::filesystem;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

namespace ov {

struct Metrics {
  std::shared_ptr<prometheus::Registry> reg = std::make_shared<prometheus::Registry>();
  prometheus::Family<prometheus::Counter>& ops =
      prometheus::BuildCounter().Name("ov_storage_ops_total").Help("Chunk operations").Register(*reg);
  prometheus::Family<prometheus::Histogram>& lat =
      prometheus::BuildHistogram().Name("ov_storage_op_seconds").Help("Chunk operation latency").Register(*reg);
  prometheus::Family<prometheus::Gauge>& g =
      prometheus::BuildGauge().Name("ov_storage_usage").Help("Stored chunks and bytes").Register(*reg);
  prometheus::Family<prometheus::Counter>& bytes =
      prometheus::BuildCounter().Name("ov_storage_bytes_total").Help("Bytes written/read").Register(*reg);
  prometheus::Counter& checksum_failures =
      prometheus::BuildCounter().Name("ov_storage_checksum_failures_total").Help("Chunks failing verification")
          .Register(*reg).Add({});
  prometheus::Gauge& chunks = g.Add({{"kind", "chunks"}});
  prometheus::Gauge& used = g.Add({{"kind", "bytes"}});
  prometheus::Counter& bytes_in = bytes.Add({{"dir", "in"}});
  prometheus::Counter& bytes_out = bytes.Add({{"dir", "out"}});

  prometheus::Histogram& hist(const std::string& op) {
    static const prometheus::Histogram::BucketBoundaries b{.0005, .001, .002, .005, .01, .02, .05, .1, .25, .5, 1};
    return lat.Add({{"op", op}}, b);
  }
};

class StorageNode final : public objectvault::Storage::Service {
 public:
  StorageNode(std::string id, fs::path dir, bool fsync_writes, Metrics& m)
      : id_(std::move(id)), dir_(std::move(dir)), fsync_(fsync_writes), m_(m),
        put_lat_(m.hist("put")), get_lat_(m.hist("get")), del_lat_(m.hist("delete")) {
    fs::create_directories(dir_ / "tmp");
    fs::create_directories(dir_ / "quarantine");
    for (auto& e : fs::recursive_directory_iterator(dir_)) {
      if (!e.is_regular_file()) continue;
      auto parent = e.path().parent_path().filename().string();
      if (parent == "tmp") {
        fs::remove(e.path());  // incomplete writes from a crash
        continue;
      }
      if (parent == "quarantine" || e.path().parent_path() == dir_) continue;
      chunks_++;
      bytes_ += e.file_size() - 32;
    }
    m_.chunks.Set(chunks_);
    m_.used.Set(bytes_);
    std::cerr << "[" << id_ << "] recovered " << chunks_ << " chunks (" << bytes_ << " bytes) from " << dir_ << "\n";
  }

  // Background scrubber: re-verifies every chunk's SHA-256 at a bounded rate
  // so silent corruption is found even on replicas that are never read.
  void start_scrubber(std::chrono::seconds every, double max_mb_per_sec) {
    std::thread([this, every, max_mb_per_sec] {
      for (;;) {
        std::this_thread::sleep_for(every);
        auto t0 = std::chrono::steady_clock::now();
        uint64_t scanned = 0, bad = 0;
        std::error_code ec;
        for (auto it = fs::recursive_directory_iterator(dir_, ec); it != fs::recursive_directory_iterator(); it.increment(ec)) {
          if (ec || !it->is_regular_file()) continue;
          auto parent = it->path().parent_path().filename().string();
          if (parent == "tmp" || parent == "quarantine" || it->path().parent_path() == dir_) continue;
          std::string id = it->path().filename().string();
          std::string stored, data;
          if (!read_file(it->path(), &stored, &data)) continue;  // deleted meanwhile
          scanned += data.size();
          Digest d = sha256(data);
          if (std::memcmp(d.data(), stored.data(), 32) != 0) {
            quarantine(id, data.size());
            bad++;
          }
          // rate limit
          double want = scanned / (max_mb_per_sec * 1e6);
          double have = seconds_since(t0);
          if (want > have) std::this_thread::sleep_for(std::chrono::duration<double>(want - have));
        }
        scrub_passes_.Increment();
        if (bad) std::cerr << "[" << id_ << "] scrub found " << bad << " corrupt chunk(s)\n";
      }
    }).detach();
  }

  Status PutChunk(ServerContext*, const objectvault::PutChunkRequest* req,
                  objectvault::PutChunkResponse*) override {
    auto t0 = std::chrono::steady_clock::now();
    if (!valid_id(req->chunk_id())) return Status(StatusCode::INVALID_ARGUMENT, "bad chunk id");
    Digest d = sha256(req->data());
    if (req->sha256().size() != 32 || std::memcmp(d.data(), req->sha256().data(), 32) != 0) {
      m_.checksum_failures.Increment();
      return Status(StatusCode::DATA_LOSS, "checksum mismatch on write");
    }
    fs::path final_path = path_for(req->chunk_id());
    bool existed = fs::exists(final_path);
    fs::create_directories(final_path.parent_path());
    fs::path tmp = dir_ / "tmp" / (req->chunk_id() + "." + random_id());
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return Status(StatusCode::INTERNAL, std::strerror(errno));
    bool ok = write_all(fd, reinterpret_cast<const char*>(d.data()), 32) &&
              write_all(fd, req->data().data(), req->data().size());
    if (ok && fsync_) ok = ::fsync(fd) == 0;
    ::close(fd);
    if (!ok) {
      fs::remove(tmp);
      return Status(StatusCode::INTERNAL, "write failed");
    }
    fs::rename(tmp, final_path);
    if (fsync_) {
      // Persist the rename, then flush the device cache (shared across writers).
      int dfd = ::open(final_path.parent_path().c_str(), O_RDONLY);
      if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
      }
      if (!sync_.barrier()) return Status(StatusCode::INTERNAL, "device flush failed");
    }
    if (!existed) {
      chunks_++;
      bytes_ += req->data().size();
      m_.chunks.Set(chunks_);
      m_.used.Set(bytes_);
    }
    m_.bytes_in.Increment(req->data().size());
    put_lat_.Observe(seconds_since(t0));
    return Status::OK;
  }

  Status GetChunk(ServerContext*, const objectvault::GetChunkRequest* req,
                  objectvault::GetChunkResponse* resp) override {
    auto t0 = std::chrono::steady_clock::now();
    if (!valid_id(req->chunk_id())) return Status(StatusCode::INVALID_ARGUMENT, "bad chunk id");
    fs::path p = path_for(req->chunk_id());
    std::string stored;
    std::string* data = resp->mutable_data();
    if (!read_file(p, &stored, data)) return Status(StatusCode::NOT_FOUND, "no such chunk");
    Digest d = sha256(*data);
    if (std::memcmp(d.data(), stored.data(), 32) != 0) {
      // Bit rot or tampering: quarantine so repair re-replicates a good copy.
      quarantine(req->chunk_id(), data->size());
      return Status(StatusCode::DATA_LOSS, "checksum mismatch on read");
    }
    resp->set_sha256(stored);
    m_.bytes_out.Increment(static_cast<double>(data->size()));
    get_lat_.Observe(seconds_since(t0));
    return Status::OK;
  }

  Status DeleteChunk(ServerContext*, const objectvault::DeleteChunkRequest* req,
                     objectvault::DeleteChunkResponse*) override {
    auto t0 = std::chrono::steady_clock::now();
    if (!valid_id(req->chunk_id())) return Status(StatusCode::INVALID_ARGUMENT, "bad chunk id");
    fs::path p = path_for(req->chunk_id());
    std::error_code ec;
    auto size = fs::file_size(p, ec);
    if (!ec && fs::remove(p, ec)) {
      chunks_--;
      bytes_ -= size - 32;
      m_.chunks.Set(chunks_);
      m_.used.Set(bytes_);
    }
    del_lat_.Observe(seconds_since(t0));
    return Status::OK;  // idempotent
  }

  Status Stat(ServerContext*, const objectvault::StatRequest*, objectvault::StatResponse* resp) override {
    resp->set_node_id(id_);
    resp->set_chunks(chunks_);
    resp->set_bytes_used(bytes_);
    resp->set_checksum_failures(static_cast<uint64_t>(m_.checksum_failures.Value()));
    std::lock_guard<std::mutex> g(qmu_);
    for (auto& id : newly_quarantined_) resp->add_quarantined(id);
    newly_quarantined_.clear();
    return Status::OK;
  }

 private:
  static bool read_file(const fs::path& p, std::string* stored, std::string* data) {
    std::ifstream in(p, std::ios::binary);
    if (!in) return false;
    in.seekg(0, std::ios::end);
    auto total = static_cast<size_t>(in.tellg());
    if (total < 32) return false;
    in.seekg(0);
    stored->resize(32);
    in.read(stored->data(), 32);
    data->resize(total - 32);
    in.read(data->data(), static_cast<std::streamsize>(total - 32));
    return static_cast<bool>(in);
  }

  void quarantine(const std::string& id, size_t size) {
    m_.checksum_failures.Increment();
    std::error_code ec;
    fs::rename(path_for(id), dir_ / "quarantine" / id, ec);
    if (ec) return;
    chunks_--;
    bytes_ -= static_cast<int64_t>(size);
    m_.chunks.Set(chunks_);
    m_.used.Set(bytes_);
    std::lock_guard<std::mutex> g(qmu_);
    newly_quarantined_.push_back(id);
  }

  static bool valid_id(const std::string& id) {
    if (id.size() < 8 || id.size() > 64) return false;
    for (char c : id)
      if (!std::isxdigit(static_cast<unsigned char>(c))) return false;
    return true;
  }
  fs::path path_for(const std::string& id) const { return dir_ / id.substr(0, 2) / id; }
  static bool write_all(int fd, const char* p, size_t n) {
    while (n > 0) {
      ssize_t w = ::write(fd, p, n);
      if (w < 0) {
        if (errno == EINTR) continue;
        return false;
      }
      p += w;
      n -= static_cast<size_t>(w);
    }
    return true;
  }

  std::string id_;
  fs::path dir_;
  bool fsync_;
  Metrics& m_;
  prometheus::Histogram& put_lat_;
  prometheus::Histogram& get_lat_;
  prometheus::Histogram& del_lat_;
  std::atomic<int64_t> chunks_{0};
  std::atomic<int64_t> bytes_{0};
  GroupSync sync_{(dir_ / "barrier").string()};
  std::mutex qmu_;
  std::vector<std::string> newly_quarantined_;
  prometheus::Counter& scrub_passes_ =
      prometheus::BuildCounter().Name("ov_storage_scrub_passes_total").Help("Completed scrub passes").Register(*m_.reg).Add({});
};

}  // namespace ov

int main(int argc, char** argv) {
  std::string id = "s1", listen = "0.0.0.0:9001", dir = "data/s1", metrics = "0.0.0.0:9201";
  bool fsync_writes = true;
  int scrub_every = 60;
  double scrub_mbps = 200;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--id") id = next();
    else if (a == "--listen") listen = next();
    else if (a == "--dir") dir = next();
    else if (a == "--metrics") metrics = next();
    else if (a == "--no-fsync") fsync_writes = false;
    else if (a == "--scrub-interval") scrub_every = std::stoi(next());
    else if (a == "--scrub-rate-mb") scrub_mbps = std::stod(next());
    else {
      std::cerr << "usage: ov-storage --id ID --listen HOST:PORT --dir DIR --metrics HOST:PORT [--no-fsync]\n"
                   "                  [--scrub-interval SECONDS (0=off)] [--scrub-rate-mb MB/s]\n";
      return 2;
    }
  }
  ov::Metrics m;
  prometheus::Exposer exposer{metrics};
  exposer.RegisterCollectable(m.reg);
  ov::StorageNode node(id, dir, fsync_writes, m);
  if (scrub_every > 0) node.start_scrubber(std::chrono::seconds(scrub_every), scrub_mbps);

  grpc::ServerBuilder b;
  b.AddListeningPort(listen, grpc::InsecureServerCredentials());
  b.SetMaxReceiveMessageSize(64 << 20);
  b.SetMaxSendMessageSize(64 << 20);
  b.RegisterService(&node);
  auto server = b.BuildAndStart();
  std::cerr << "[" << id << "] storage node listening on " << listen << ", metrics on " << metrics << "\n";
  server->Wait();
}
