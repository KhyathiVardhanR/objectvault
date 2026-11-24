#pragma once

// Replica manager. Restores the replication factor after failures:
//   * node down  -> every chunk with a replica on that node is copied from a
//                   healthy replica to the next node on the hash ring;
//   * corruption -> a replica that failed checksum verification (or is
//                   missing) is queued and rewritten from a good copy;
//   * partial writes that only reached the write quorum are queued too.

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>

#include "gateway/cluster.h"
#include "gateway/metadata.h"

namespace ov {

struct RepairMetrics {
  prometheus::Counter& repaired;
  prometheus::Counter& repaired_bytes;
  prometheus::Counter& lost;
  prometheus::Gauge& under_replicated;
  prometheus::Gauge& last_recovery_seconds;
};

class RepairManager {
 public:
  RepairManager(Cluster& c, MetaStore& m, RepairMetrics rm) : cluster_(c), meta_(m), rm_(rm) {}

  void node_changed(const std::string& node, bool up) {
    std::lock_guard<std::mutex> g(mu_);
    if (!up) {
      std::cerr << "[repair] node " << node << " is DOWN; re-replicating its chunks\n";
      if (!degraded_) degraded_since_ = std::chrono::steady_clock::now();
      degraded_ = true;
    } else {
      std::cerr << "[repair] node " << node << " is back UP\n";
    }
  }

  void start() {
    std::thread([this] { loop(); }).detach();
  }

  double last_recovery_seconds() const { return last_recovery_.load(); }
  int64_t under_replicated() const { return under_.load(); }

 private:
  void loop() {
    for (;;) {
      try {
        int64_t pending = 0;
        size_t worked = 0;
        auto down = cluster_.down_nodes();
        for (const auto& node : down) {
          auto tasks = meta_.chunks_on_node(node, 512);
          worked += run(tasks, node);
          pending += meta_.count_replicas_on(node);
        }
        auto queued = meta_.take_repair_queue(512);
        worked += run(queued, "");
        pending += meta_.repair_queue_size();
        under_ = pending;
        rm_.under_replicated.Set(static_cast<double>(pending));

        std::lock_guard<std::mutex> g(mu_);
        if (degraded_ && pending == 0) {
          double secs = seconds_since(degraded_since_);
          last_recovery_ = secs;
          rm_.last_recovery_seconds.Set(secs);
          std::cerr << "[repair] full redundancy restored " << secs << "s after failure\n";
          degraded_ = false;
        }
        if (worked == 0)
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
      } catch (const std::exception& e) {
        std::cerr << "[repair] error: " << e.what() << "\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
  }

  size_t run(const std::vector<ReplicaTask>& tasks, const std::string& failed_node) {
    std::vector<std::future<void>> futs;
    for (const auto& t : tasks)
      futs.push_back(cluster_.submit([this, t, failed_node] { repair_one(t, failed_node); }));
    for (auto& f : futs) f.get();
    return tasks.size();
  }

  // Copy a chunk from a healthy replica to a node that doesn't hold it.
  void repair_one(const ReplicaTask& t, const std::string& failed_node) {
    auto down = cluster_.down_nodes();
    std::vector<std::string> sources;
    for (const auto& n : t.nodes)
      if (n != failed_node && !down.count(n)) sources.push_back(n);
    std::set<std::string> holders(t.nodes.begin(), t.nodes.end());

    // Target: next ring node that is up and doesn't already hold the chunk.
    std::string target;
    for (const auto& n : cluster_.ring().place(t.chunk_id, cluster_.ring().nodes().size(), down)) {
      if (!holders.count(n)) {
        target = n;
        break;
      }
    }
    if (sources.empty() || target.empty()) {
      if (sources.empty()) {
        rm_.lost.Increment();
        std::cerr << "[repair] chunk " << t.chunk_id << " has no healthy replica\n";
      }
      // Nothing we can do now (e.g. not enough live nodes); drop the stale
      // replica record so we don't spin, the chunk stays readable from others.
      if (!failed_node.empty()) meta_.drop_replica(t.chunk_id, failed_node);
      return;
    }
    Digest expect{};
    for (size_t i = 0; i < 32; i++) expect[i] = static_cast<unsigned char>(std::stoi(t.sha256_hex.substr(2 * i, 2), nullptr, 16));
    std::string data;
    bool ok = cluster_.read_verified(t.chunk_id, expect, sources, &data, [this, &t](const std::string& bad) {
      meta_.enqueue_repair(t.chunk_id, bad);
    });
    if (!ok) {
      rm_.lost.Increment();
      return;
    }
    if (!cluster_.put(target, t.chunk_id, data, expect).ok()) return;  // retried next round
    meta_.move_replica(t.chunk_id, failed_node, target);
    rm_.repaired.Increment();
    rm_.repaired_bytes.Increment(static_cast<double>(data.size()));
  }

  Cluster& cluster_;
  MetaStore& meta_;
  RepairMetrics rm_;
  std::mutex mu_;
  bool degraded_ = false;
  std::chrono::steady_clock::time_point degraded_since_;
  std::atomic<double> last_recovery_{0};
  std::atomic<int64_t> under_{0};
};

}  // namespace ov
