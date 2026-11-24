#pragma once

// Group commit for durability barriers on a storage node.
//
// fsync(2) on macOS only hands data to the drive, which may keep it in a
// volatile cache; a true flush needs fcntl(F_FULLFSYNC), which flushes the
// whole device cache and is expensive (a few hundred per second). Each writer
// therefore fsync(2)s its own file and then calls barrier(): concurrent
// writers share one F_FULLFSYNC that started after their fsync returned.
// On Linux, fsync(2) already flushes the device cache and barrier() is free.

#include <fcntl.h>
#include <unistd.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

namespace ov {

class GroupSync {
 public:
  explicit GroupSync(const std::string& path) { fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644); }
  ~GroupSync() {
    if (fd_ >= 0) ::close(fd_);
  }

  bool barrier() {
#ifdef __APPLE__
    std::unique_lock<std::mutex> lk(mu_);
    uint64_t my = ++requested_;
    while (completed_ < my) {
      if (running_) {
        cv_.wait(lk);
        continue;
      }
      running_ = true;
      uint64_t target = requested_;
      lk.unlock();
      bool ok = ::fcntl(fd_, F_FULLFSYNC) == 0;
      lk.lock();
      running_ = false;
      completed_ = target;
      ok_ = ok;
      flushes_++;
      cv_.notify_all();
    }
    return ok_;
#else
    return true;
#endif
  }

  uint64_t flushes() const { return flushes_; }

 private:
  int fd_ = -1;
  std::mutex mu_;
  std::condition_variable cv_;
  uint64_t requested_ = 0, completed_ = 0, flushes_ = 0;
  bool running_ = false, ok_ = true;
};

}  // namespace ov
