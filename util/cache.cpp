// File   cache.cpp
// Author lidongming
// Date   2019-02-28 17:13:56
// Brief

#include "util/cache.h"

namespace feature_engine {

std::vector<boost::lockfree::spsc_queue<
  FeatureCacheItem, boost::lockfree::capacity<10000>>>
    g_feature_cache_queues(std::thread::hardware_concurrency());

}  // namespace feature_engine
