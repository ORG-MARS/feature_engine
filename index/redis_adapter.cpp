// File   redis_adapter.cpp
// Author lidongming
// Date   2018-10-11 11:37:58
// Brief

#include "feature_engine/index/redis_adapter.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

using namespace commonlib;

RedisAdapter::RedisAdapter(const std::string& redis_host, int redis_port,
  const std::string& password, int timeout_ms) :
  redis_cluster_client_(NULL), redis_client_(NULL) {
  if (FLAGS_enable_redis_cluster) {
    redis_cluster_client_ = new RedisClusterClient(redis_host, redis_port); 
  } else {
    redis_client_ = new RedisClient(redis_host, redis_port, password, timeout_ms); 
  }
}

}  // namespace feature_engine
