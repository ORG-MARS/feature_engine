// File   task.h
// Author lidongming
// Date   2018-09-11 20:41:46
// Brief

#ifndef FEATURE_ENGINE_SERVER_SERVER_TASK_H_
#define FEATURE_ENGINE_SERVER_SERVER_TASK_H_

#include <atomic>
#include <thread>
#include <mutex>
#include <functional>
#include <condition_variable>
// #include "prediction_server/deps/glog/include/glog/logging.h"

namespace feature_engine {

#if 0
struct TaskTracker {
  std::string rid;
  int total_task_count;
  std::atomic<int> finish_task_count;
  std::mutex notify_mutex;
  std::condition_variable condition;

  TaskTracker() {
    total_task_count = 0;
    finish_task_count.store(0);
  }

  void done() {
    finish_task_count.fetch_add(1);
    if (finish_task_count.load() >= total_task_count) {
      // LOG(INFO) << "prepare notify rid:" << rid;
      std::unique_lock<std::mutex> lock(notify_mutex);
      condition.notify_one();
      // LOG(INFO) << "finish notify rid:" << rid;
    }
  }

  void wait() {
    std::unique_lock<std::mutex> lock(notify_mutex);
    condition.wait(lock);
  }
};
#endif

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_SERVER_SERVER_TASK_H_
