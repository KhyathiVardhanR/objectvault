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
#include <string>

#include "common/util.h"
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
      if (parent == "quarantine") continue;
      chunks_++;
      bytes_ += e.file_size() - 32;
    }
    m_.chunks.Set(chunks_);
    m_.used.Set(bytes_);
    std::cerr << "[" << id_ << "] recovered " << chunks_ << " chunks (" << bytes_ << " bytes) from " << dir_ << "\n";
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
    std::ifstream in(p, std::ios::binary);
    if (!in) return Status(StatusCode::NOT_FOUND, "no such chunk");
    std::string stored(32, '\0');
    in.read(stored.data(), 32);
    std::string* data = resp->mutable_data();
    in.seekg(0, std::ios::end);
    auto size = static_cast<size_t>(in.tellg()) - 32;
    in.seekg(32);
    data->resize(size);
    in.read(data->data(), size);
    Digest d = sha256(*data);
    if (!in || std::memcmp(d.data(), stored.data(), 32) != 0) {
      // Bit rot or tampering: quarantine so repair re-replicates a good copy.
      m_.checksum_failures.Increment();
      std::error_code ec;
      fs::rename(p, dir_ / "quarantine" / req->chunk_id(), ec);
      if (!ec) {
        chunks_--;
        bytes_ -= size;
      }
      return Status(StatusCode::DATA_LOSS, "checksum mismatch on read");
    }
    resp->set_sha256(stored);
    m_.bytes_out.Increment(size);
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
    return Status::OK;
  }

 private:
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
};

}  // namespace ov

int main(int argc, char** argv) {
  std::string id = "s1", listen = "0.0.0.0:9001", dir = "data/s1", metrics = "0.0.0.0:9201";
  bool fsync_writes = true;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--id") id = next();
    else if (a == "--listen") listen = next();
    else if (a == "--dir") dir = next();
    else if (a == "--metrics") metrics = next();
    else if (a == "--no-fsync") fsync_writes = false;
    else {
      std::cerr << "usage: ov-storage --id ID --listen HOST:PORT --dir DIR --metrics HOST:PORT [--no-fsync]\n";
      return 2;
    }
  }
  ov::Metrics m;
  prometheus::Exposer exposer{metrics};
  exposer.RegisterCollectable(m.reg);
  ov::StorageNode node(id, dir, fsync_writes, m);

  grpc::ServerBuilder b;
  b.AddListeningPort(listen, grpc::InsecureServerCredentials());
  b.SetMaxReceiveMessageSize(64 << 20);
  b.SetMaxSendMessageSize(64 << 20);
  b.RegisterService(&node);
  auto server = b.BuildAndStart();
  std::cerr << "[" << id << "] storage node listening on " << listen << ", metrics on " << metrics << "\n";
  server->Wait();
}
