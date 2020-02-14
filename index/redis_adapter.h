// File   redis_adapter.h
// Author lidongming
// Date   2018-10-11 11:34:31
// Brief

#ifndef FEATURE_ENGINE_INDEX_REDIS_ADAPTER_H_
#define FEATURE_ENGINE_INDEX_REDIS_ADAPTER_H_

// #include <memory>
#include <string>
#include "feature_engine/deps/commonlib/include/redis_cluster_client.h"
#include "feature_engine/deps/commonlib/include/redis_client.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

class RedisAdapter {
 public:
  RedisAdapter(const std::string& redis_host, int redis_port,
      const std::string& password, int timeout_ms = 1000);
  ~RedisAdapter() {
    if (redis_cluster_client_) {
      delete redis_cluster_client_;
    }
    if (redis_client_) {
      delete redis_client_;
    }
  }

  std::string get(const std::string& key) const {
    if (FLAGS_enable_redis_cluster) {
      return redis_cluster_client_->get(key);
    } else {
      return redis_client_->get(key);
    } 
  }
  int mget(const std::string& prefix, const std::vector<std::string>& keys,
      std::unordered_map<std::string, std::string>& res) const {
    if (FLAGS_enable_redis_cluster) {
      return redis_cluster_client_->mget(prefix, keys, res);
    } else {
      return redis_client_->mget(prefix, keys, res);
    }
  }

 private:
  // std::shared_ptr<commonlib::RedisClusterClient> redis_cluster_client_;
  // std::shared_ptr<commonlib::RedisClient> redis_client_;
  commonlib::RedisClusterClient* redis_cluster_client_;
  commonlib::RedisClient* redis_client_;
};  // RedisAdapter

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_REDIS_ADAPTER_H_
