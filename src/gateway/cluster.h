#pragma once

// Client side of the storage tier: one gRPC channel per storage node, quorum
// replicated chunk writes, checksum-verified reads that fail over between
// replicas, and heartbeat-based node health.

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <cstring>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common/ring.h"
#include "common/util.h"
#include "storage.grpc.pb.h"

namespace ov {

struct NodeSpec {
  std::string id;
  std::string addr;
};

class Cluster {
 public:
  Cluster(const std::vector<NodeSpec>& nodes, size_t replicas, size_t write_quorum, size_t io_threads)
      : ring_(ids(nodes)), replicas_(replicas), quorum_(write_quorum), pool_(io_threads) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(64 << 20);
    args.SetMaxSendMessageSize(64 << 20);
    for (const auto& n : nodes) {
      auto ch = grpc::CreateCustomChannel(n.addr, grpc::InsecureChannelCredentials(), args);
      stubs_[n.id] = objectvault::Storage::NewStub(ch);
      up_[n.id] = true;
      misses_[n.id] = 0;
    }
  }

  size_t replicas() const { return replicas_; }
  const HashRing& ring() const { return ring_; }

  std::set<std::string> down_nodes() const {
    std::lock_guard<std::mutex> g(mu_);
    std::set<std::string> out;
    for (auto& [id, up] : up_)
      if (!up) out.insert(id);
    return out;
  }
  bool is_up(const std::string& id) const {
    std::lock_guard<std::mutex> g(mu_);
    auto it = up_.find(id);
    return it != up_.end() && it->second;
  }

  // Run fn on the I/O pool and return a future.
  template <class F>
  auto submit(F fn) -> std::future<decltype(fn())> {
    auto task = std::make_shared<std::packaged_task<decltype(fn())()>>(std::move(fn));
    auto fut = task->get_future();
    boost::asio::post(pool_, [task] { (*task)(); });
    return fut;
  }

  grpc::Status put(const std::string& node, const std::string& id, std::string_view data, const Digest& d,
                   std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    objectvault::PutChunkRequest req;
    req.set_chunk_id(id);
    req.set_data(data.data(), data.size());
    req.set_sha256(digest_bytes(d));
    objectvault::PutChunkResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + timeout);
    return stub(node)->PutChunk(&ctx, req, &resp);
  }

  grpc::Status get(const std::string& node, const std::string& id, std::string* out,
                   std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    objectvault::GetChunkRequest req;
    req.set_chunk_id(id);
    objectvault::GetChunkResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + timeout);
    auto st = stub(node)->GetChunk(&ctx, req, &resp);
    if (st.ok()) *out = std::move(*resp.mutable_data());
    return st;
  }

  void remove(const std::string& node, const std::string& id) {
    objectvault::DeleteChunkRequest req;
    req.set_chunk_id(id);
    objectvault::DeleteChunkResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    stub(node)->DeleteChunk(&ctx, req, &resp);
  }

  // Writes a chunk to its R placement nodes in parallel. Returns the nodes
  // that acknowledged, or nullopt if fewer than the write quorum did.
  std::optional<std::vector<std::string>> write_replicated(const std::string& id, const std::string& data,
                                                           const Digest& d) {
    auto targets = ring_.place(id, replicas_, down_nodes());
    std::vector<std::future<grpc::Status>> futs;
    for (const auto& n : targets) futs.push_back(submit([this, n, &id, &data, &d] { return put(n, id, data, d); }));
    std::vector<std::string> acked;
    for (size_t i = 0; i < targets.size(); i++) {
      auto st = futs[i].get();
      if (st.ok()) acked.push_back(targets[i]);
      else note_failure(targets[i], st);
    }
    if (acked.size() < quorum_) return std::nullopt;
    return acked;
  }

  struct PendingChunk {
    std::string id;
    std::string_view data;
    Digest digest;
    std::vector<std::string> acked;  // filled in
  };

  // Writes many chunks at once: every (chunk, replica) put is submitted to the
  // I/O pool up front, so large objects upload with full parallelism. Must not
  // be called from inside the pool. Returns false if any chunk misses quorum.
  bool write_batch(std::vector<PendingChunk>& chunks) {
    auto down = down_nodes();
    std::vector<std::vector<std::string>> targets(chunks.size());
    std::vector<std::vector<std::future<grpc::Status>>> futs(chunks.size());
    for (size_t i = 0; i < chunks.size(); i++) {
      targets[i] = ring_.place(chunks[i].id, replicas_, down);
      for (const auto& n : targets[i]) {
        auto* c = &chunks[i];
        futs[i].push_back(submit([this, n, c] {
          return put(n, c->id, c->data, c->digest);
        }));
      }
    }
    bool ok = true;
    for (size_t i = 0; i < chunks.size(); i++) {
      for (size_t j = 0; j < targets[i].size(); j++) {
        if (futs[i][j].get().ok()) chunks[i].acked.push_back(targets[i][j]);
      }
      if (chunks[i].acked.size() < quorum_) ok = false;
    }
    return ok;
  }

  // Reads a chunk from the first healthy replica whose data verifies.
  // Replicas that fail verification are reported through on_corrupt.
  bool read_verified(const std::string& id, const Digest& expect, const std::vector<std::string>& nodes,
                     std::string* out, const std::function<void(const std::string&)>& on_corrupt) {
    // Healthy replicas first, in random order (spreads read load and makes
    // reads exercise every replica); down nodes last as a fallback.
    std::vector<std::string> order;
    for (const auto& n : nodes)
      if (is_up(n)) order.push_back(n);
    thread_local std::mt19937 rng{std::random_device{}()};
    std::shuffle(order.begin(), order.end(), rng);
    for (const auto& n : nodes)
      if (!is_up(n)) order.push_back(n);
    for (const auto& n : order) {
      auto st = get(n, id, out);
      if (st.ok()) {
        Digest d = sha256(*out);
        if (std::memcmp(d.data(), expect.data(), 32) == 0) return true;
        on_corrupt(n);
        continue;
      }
      if (st.error_code() == grpc::StatusCode::DATA_LOSS || st.error_code() == grpc::StatusCode::NOT_FOUND) {
        on_corrupt(n);
      } else {
        note_failure(n, st);
      }
    }
    return false;
  }

  // Heartbeats every node; a node is down after `threshold` consecutive misses.
  void start_heartbeats(std::chrono::milliseconds every, int threshold,
                        std::function<void(const std::string&, bool)> on_change,
                        std::function<void(const std::string&, const std::string&)> on_quarantined) {
    hb_ = std::thread([=, this] {
      for (;;) {
        for (auto& [id, stub] : stubs_) {
          objectvault::StatRequest req;
          objectvault::StatResponse resp;
          grpc::ClientContext ctx;
          ctx.set_deadline(std::chrono::system_clock::now() + every);
          bool ok = stub->Stat(&ctx, req, &resp).ok();
          if (ok)
            for (const auto& q : resp.quarantined()) on_quarantined(id, q);
          bool changed = false, now_up = false;
          {
            std::lock_guard<std::mutex> g(mu_);
            if (ok) {
              misses_[id] = 0;
              if (!up_[id]) up_[id] = changed = now_up = true;
            } else if (++misses_[id] >= threshold && up_[id]) {
              up_[id] = false;
              changed = true;
            }
          }
          if (changed) on_change(id, now_up);
        }
        std::this_thread::sleep_for(every);
      }
    });
    hb_.detach();
  }

  std::vector<std::string> node_ids() const { return ring_.nodes(); }

 private:
  static std::vector<std::string> ids(const std::vector<NodeSpec>& nodes) {
    std::vector<std::string> out;
    for (auto& n : nodes) out.push_back(n.id);
    return out;
  }
  objectvault::Storage::Stub* stub(const std::string& node) { return stubs_.at(node).get(); }
  void note_failure(const std::string&, const grpc::Status&) {}

  HashRing ring_;
  size_t replicas_, quorum_;
  boost::asio::thread_pool pool_;
  std::map<std::string, std::unique_ptr<objectvault::Storage::Stub>> stubs_;
  mutable std::mutex mu_;
  std::map<std::string, bool> up_;
  std::map<std::string, int> misses_;
  std::thread hb_;
};

}  // namespace ov
