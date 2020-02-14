// File   cache.h
// Author lidongming
// Date   2019-02-25 16:36:56
// Brief

#ifndef FEATURE_ENGINE_UTIL_CACHE_H_
#define FEATURE_ENGINE_UTIL_CACHE_H_

#include <atomic>
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_set>
#include <sstream>
#include <boost/lockfree/spsc_queue.hpp> // boost::lockfree::spsc_queue

#include "deps/glog/include/glog/logging.h"
#include "deps/gflags/include/gflags/gflags.h"
#include "deps/smalltable/include/smalltable.hpp" // ST_TABLE/VersionManager
#include "deps/commonlib/include/time_utils.h"
#include "proto/feature.pb.h"
#include "common/common_define.h"
#include "common/common_gflags.h"

namespace feature_engine {

//----------------------------------Cache Item----------------------------------
// 缓存及队列中的基本数据结构
struct CacheItem {
  std::string id;
  int64_t update_time;  // milli-seconds
  int expire_seconds;  // seconds

  CacheItem() : update_time(0), expire_seconds(0) {}
  virtual ~CacheItem() {}

  CacheItem(const std::string& id_) : id(id_) {}
  CacheItem(const std::string& cache_id, size_t expire)
    : id(cache_id), expire_seconds(expire) {
    update_time = commonlib::TimeUtils::GetCurrentTime();
  }

  bool Expired() {
    int64_t current_time = commonlib::TimeUtils::GetCurrentTime();
    return ((current_time - update_time) / 1000 > expire_seconds);
  }

  const std::string Dump() const {
    std::stringstream ss;
    ss << "id:" << id << " update_time:" << update_time
       << " expire_seconds:" << expire_seconds;
    return ss.str();
  }

  virtual void Release() {}

  bool operator==(const CacheItem& item) const {
    return this->id == item.id;
  } 
};

// 缓存用户特征、物料特征
struct FeatureCacheItem : public CacheItem {
  FeatureCacheItem() : CacheItem() {}
  std::shared_ptr<feature_proto::FeaturesMap> feature_map;
  void Release() override {} 

  bool operator==(const FeatureCacheItem& item) const {
    return this->id == item.id;
  } 
};

}

namespace std {
 // Hash for CacheItem
 template<>
 struct hash<feature_engine::CacheItem> {
   typedef feature_engine::CacheItem argument_type;
   typedef std::size_t result_type;
   result_type operator()(argument_type const& s) const {
     return std::hash<std::string>{}(s.id);
   }
 };

 // Hash for FeatureCacheItem
 template<>
 struct hash<feature_engine::FeatureCacheItem> {
   typedef feature_engine::FeatureCacheItem argument_type;
   typedef std::size_t result_type;
   result_type operator()(argument_type const& s) const {
     return std::hash<std::string>{}(s.id);
   }
 };
}

namespace feature_engine {

//----------------------------------Smalltable----------------------------------
// 缓存的底层存储及版本切换由smalltable实现
// SmallTable definitions for CacheTable 
DEFINE_ATTRIBUTE(CACHE_ID, std::string);
DEFINE_ATTRIBUTE(CACHE_ITEM, CacheItem);
typedef ST_TABLE(CACHE_ID, CACHE_ITEM, ST_UNIQUE_KEY(CACHE_ID)) CacheSTTable;

// SmallTable definitions for FeatureCacheTable 
DEFINE_ATTRIBUTE(FEATURE_CACHE_ITEM, FeatureCacheItem);
typedef ST_TABLE(CACHE_ID, FEATURE_CACHE_ITEM, ST_UNIQUE_KEY(CACHE_ID))
  FeatureCacheSTTable;

//----------------------------------CacheTable----------------------------------
// 缓存需支持过期删除
class CacheTableInterface {
 public:
   virtual void RemoveExpired() = 0;

 private:
};

// Cache implement by smalltable
// 用于缓存普通数据，支持过期删除、容量限制
// WARNING:not thread safe, using version manager to put/get items from cache
// FIXME(lidongming):make this class template
class CacheTable : public CacheTableInterface {
 public:
  typedef CacheSTTable st_table_type;

  CacheTable() { Init(); }

  int Init() { return cache_st_table_.init(); }

  void Put(const CacheItem& cache_item) {
    cache_st_table_.insert(cache_item.id, cache_item);
  }

  bool Get(const std::string& cache_id, CacheItem* cache_item) {
    auto it = cache_st_table_.seek<CACHE_ID>(cache_id);
    if (it != (long)NULL) {
      *cache_item = it->at<CACHE_ITEM>();
      return true;
    }
    return false;
  }

  CacheSTTable& cache_st_table() { return cache_st_table_; }

  // 超过缓存容量时删除多余的数据
  void StripTable(int32_t cache_max_size) {
    // Remove items when reach cache limit
    int eliminate_size = cache_st_table_.size() - cache_max_size;
    if (eliminate_size > 0) {
      eliminate_size *= 1.5;  // Remove more items from cache
      std::vector<CacheItem> eliminate_items;
      CacheSTTable::Iterator it = cache_st_table_.begin();
      for (; it != cache_st_table_.end() && eliminate_size > 0;
          ++it && --eliminate_size) {
        eliminate_items.emplace_back(std::move(it->at<CACHE_ITEM>()));
      }
      for (CacheItem& item : eliminate_items) {
        cache_st_table_.erase<CACHE_ID>(item.id);
        item.Release();
      }
      // LOG(INFO) << "eliminate cache size:" << eliminate_size;
    }
  }

  // 清除缓存中的过期数据
  void RemoveExpired() override {
    std::vector<CacheItem> expired_items;
    CacheSTTable::Iterator it = cache_st_table_.begin();
    for (; it != cache_st_table_.end(); ++it) {
      CacheItem& item = it->at<CACHE_ITEM>();
      if (item.Expired()) {
        expired_items.emplace_back(item);
      }
    }
    for (CacheItem& item : expired_items) {
      cache_st_table_.erase<CACHE_ID>(item.id);
      item.Release();
    }
    // LOG(INFO) << "remove expired items size:" << expired_items.size();

  }
 private:
   CacheSTTable cache_st_table_;
};

// 主要用于特征的缓存
class FeatureCacheTable : public CacheTableInterface {
 public:
  typedef FeatureCacheSTTable st_table_type;

  FeatureCacheTable() { Init(); }

  int Init() { return cache_st_table_.init(); }

  void Put(const FeatureCacheItem& cache_item) {
    cache_st_table_.insert(cache_item.id, cache_item);
  }

  bool Get(const std::string& cache_id, FeatureCacheItem* cache_item) {
    auto it = cache_st_table_.seek<CACHE_ID>(cache_id);
    if (it != (long)NULL) {
      *cache_item = const_cast<FeatureCacheItem&>(it->at<FEATURE_CACHE_ITEM>());
      return true;
    }
    return false;
  }

  FeatureCacheSTTable& cache_st_table() { return cache_st_table_; }

  // 删除多余的缓存数据以避免超过缓存的大小
  void StripTable(int32_t cache_max_size) {
    // Remove items when reach cache limit
    int eliminate_size = cache_st_table_.size() - cache_max_size;
    if (eliminate_size > 0) {
      eliminate_size *= 1.5;  // Remove more items from cache
      std::vector<FeatureCacheItem> eliminate_items;
      FeatureCacheSTTable::Iterator it = cache_st_table_.begin();
      for (; it != cache_st_table_.end() && eliminate_size > 0;
          ++it && --eliminate_size) {
        eliminate_items.emplace_back(std::move(it->at<FEATURE_CACHE_ITEM>()));
      }
      for (FeatureCacheItem& item : eliminate_items) {
        cache_st_table_.erase<CACHE_ID>(item.id);
        item.Release();
      }
    }
  }

  // 清除过期的特征缓存
  void RemoveExpired() override {
    std::vector<FeatureCacheItem> expired_items;
    FeatureCacheSTTable::Iterator it = cache_st_table_.begin();
    for (; it != cache_st_table_.end(); ++it) {
      FeatureCacheItem& item = it->at<FEATURE_CACHE_ITEM>();
      if (item.Expired()) {
        expired_items.emplace_back(item);
      }
    }
    for (FeatureCacheItem& item : expired_items) {
      cache_st_table_.erase<CACHE_ID>(item.id);
      item.Release();
    }
    // LOG(INFO) << "remove expired items size:" << expired_items.size();

  }
 private:
   FeatureCacheSTTable cache_st_table_;
};

//---------------------------------Cache Manager--------------------------------
// 队列序号为TLS类型，用于确定具体的spsc队列，以支持单产者单消费者的模式
// 不同的处理逻辑有不同的tls seq
#define DEFINE_THREAD_LOCAL_BUFFER_ATOMIC(_name_) \
  static std::atomic<int32_t> kBufferAtomic##_name_(0)

#define REGISTER_THREAD_LOCAL_BUFFER_SEQ(_name_) \
  static thread_local size_t kBufferTLSSeq##_name_ = ++kBufferAtomic##_name_

#define THREAD_LOCAL_BUFFER_SEQ(_name_) kBufferTLSSeq##_name_

// Thread Local SPSC Queues
// spsc队列数组，每个处理线程拥有一个spsc队列(由tls seq确定)
// Template Params:
//  T:队列元素的数据类型，如CacheItem
//  QUEUE_CAPACITY:队列容量。boost spsc_queue容量需在编译期确定
template <typename T, int QUEUE_CAPACITY>
class TLSQueue {
 public:
  using QueueType = boost::lockfree::spsc_queue<T,
          boost::lockfree::capacity<QUEUE_CAPACITY>>;

  TLSQueue(uint32_t item_set_size, uint32_t queue_size)
   : item_set_size_(item_set_size) {
    items_set_.rehash(item_set_size * 2);
    // cache_queues_ = new std::vector<QueueType>(queue_size);
    cache_queues_ = std::make_unique<std::vector<QueueType>>(queue_size);
  }

  virtual ~TLSQueue() {}

  //---------------------------------Queue APIs--------------------------------
  QueueType& tls_queue(size_t seq) { return (*cache_queues_)[seq]; }

  // 在tls spsc队列里添加元素
  inline void Push(size_t seq, const std::string& id) {
    QueueType& q = tls_queue(seq);
    q.push(std::move(T(id)));
  }

  inline void Push(size_t seq, T& cache_item) {
    QueueType& q = tls_queue(seq);
    q.push(cache_item);
  }

  inline void Push(size_t seq, T&& cache_item) {
    QueueType& q = tls_queue(seq);
    q.push(cache_item);
  }

  inline bool Pop(size_t seq, T* item) {
    QueueType& q = tls_queue(seq);
    if (q.pop(*item)) {  // pop is non-blocking
      return true;
    }
    return false;
  }

  // Pop items from all queues
  // 依次遍历所有spsc队列并pop元素到set
  void PopItemsFromQueues() {
    int queue_size = cache_queues_->size();
    T item;
    for (size_t i = 0; i < queue_size; i++) {
      if (items_set_.size() >= item_set_size_) {
        break;
      }
      if (Pop(i, &item)) {
        items_set_.insert(std::move(item));
      }
    }
  }

  std::unordered_set<T> &items_set() { return items_set_; }

  void clear_items_set() { items_set_.clear(); }

 private:
  uint32_t item_set_size_;
  // Each thread holds its own spsc queue seq
  // spsc queues
  std::unique_ptr<std::vector<QueueType>> cache_queues_;

  // Cache and distinct items from queues temporarily
  //  and push the batch into cache at one time
  std::unordered_set<T> items_set_;
};

// TLS队列的管理类，包括启动数据定期处理线程(RefreshRoutine)、
//  监控线程(MonitorRoutine)等
template <typename T, int QUEUE_CAPACITY>
class TLSQueueManager {
 public:
  TLSQueueManager(const std::string& name, uint32_t items_set_size,
      uint32_t tls_queue_update_size, uint32_t tls_queue_update_interval)
    : name_(name),
      tls_queue_update_size_(tls_queue_update_size),
      tls_queue_update_interval_(tls_queue_update_interval) {
    int queue_size = std::thread::hardware_concurrency() + 10;
    tls_queue_ = std::make_unique<TLSQueue<T, QUEUE_CAPACITY>>(
                  items_set_size, queue_size);
    LOG(INFO) << "init queue_manager:" << name_ << " queues_size:" << queue_size;
  }

  virtual void Init() {}
  virtual ~TLSQueueManager() {}

  // Start Routines
  virtual void Start() {
    // LOG(INFO) << "start queue manager:" << name_;
    std::thread{&TLSQueueManager::RefreshRoutine, this}.detach();
    std::thread{&TLSQueueManager::MonitorRoutine, this}.detach();
  }

  // Refresh routine in order to process items from TLS queues
  virtual void RefreshRoutine() {
    uint64_t last = commonlib::TimeUtils::GetCurrentTime();
    uint64_t current = last;
    bool should_update = false;

    while (true) {
      tls_queue_->PopItemsFromQueues();
      const std::unordered_set<T>& items_set = tls_queue_->items_set();
      if (items_set.size() >= tls_queue_update_size_) {
        should_update = true;
      }
      current = commonlib::TimeUtils::GetCurrentTime();
      if (current - last > tls_queue_update_interval_) {
        should_update = true;
      }
      if (should_update) {
        should_update = false;
        last = current;
        RefreshProcess(items_set);
        tls_queue_->clear_items_set();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  // 自定义定期处理逻辑，用于处理从各spsc queue中获取的元素
  virtual void RefreshProcess(const std::unordered_set<T>& items_set) {}
  // 监控线程
  virtual void MonitorRoutine() {}

  // 不同的线程在各自的spsc queue中新增元素
  inline void Push(size_t seq, T& cache_item) {
    return tls_queue_->Push(seq, cache_item);
  }

  inline void Push(size_t seq, T&& cache_item) {
    return tls_queue_->Push(seq, cache_item);
  }

 protected:
  std::unique_ptr<TLSQueue<T, QUEUE_CAPACITY>> tls_queue_;

 private:
  DISALLOW_COPY_AND_ASSIGN(TLSQueueManager);
  std::string name_;
  uint32_t tls_queue_update_size_;
  size_t tls_queue_update_interval_;
};
 
// 普通数据缓存管理类
template <typename T, typename TABLE, int QUEUE_CAPACITY>
class CacheManager : public TLSQueueManager<T, QUEUE_CAPACITY> {
 public:
  // item_set_size: size of items_set
  // cache_max_size: maximum size of cache
  // cache_update_size: update cache when items size reaches the limit 
  // cache_update_interval: time interval of updating cache
  CacheManager(const std::string& name,
               uint32_t items_set_size,
               uint32_t tls_queue_update_size,
               uint32_t tls_queue_update_interval,
               uint32_t cache_max_size)
   : TLSQueueManager<T, QUEUE_CAPACITY>(name, items_set_size,
                                        tls_queue_update_size,
                                        tls_queue_update_interval),
     name_(name),
     cache_max_size_(cache_max_size) {
    Init();
  }

  void Init() override {
    LOG(INFO) << "init cache_manager name:" << name_;
    InitVersionManager();
  }

  // Init cache manager with maximum 2 versions
  void InitVersionManager() {
    if (cache_table_manager_.init(2, "cache_table") != 0) {
      LOG(FATAL) << "init cache table version manager error";
    }
  }

  // Refresh routine in order to move items from TLS queues to the cache
  void RefreshProcess(const std::unordered_set<T>& items_set) override {
    std::lock_guard<std::mutex> lock(vm_mutex_);
    // create version
    int version = cache_table_manager_.create_version();
    if (version < 0) {
      LOG(ERROR) << "create cache version error";
      return;
    }

    TABLE& table = cache_table_manager_[version];
    table.StripTable(cache_max_size_);
    typename TABLE::st_table_type& st_table = table.cache_st_table();

    // update cache st table
    for (const T& item : items_set) {
      st_table.insert(item.id, std::move(item));
    }

    // freeze version
    if (cache_table_manager_.freeze_version(version) != 0) {
      cache_table_manager_.drop_version(version);
      LOG(ERROR) << "freeze cache version error";
      return;
    }
    // LOG(INFO) << "update cache name:" << name_ << " size:" << items_set.size()
    //           << " total cache size:" << st_table.size();
  }

  // Get the cache_item from cache(st_table)
  bool GetCacheItem(const std::string& id, T* cache_item) {
    int version = cache_table_manager_.find_latest_read_only();
    if (version >= 0) {
      return cache_table_manager_[version].Get(id, cache_item);
    }
    return false;
  }

  void MonitorRoutine() override {
    int version = -1;
    while (true) {
      // monitor cache size of current version
      version = cache_table_manager_.find_latest_read_only();
      if (version >= 0) {
        LOG(INFO) << "cache name:" << name_ << " size:"
                  << cache_table_manager_[version].cache_st_table().size();
      }

      // remove expired items from cache and release resources
      RemoveExpired();

      std::this_thread::sleep_for(
          std::chrono::milliseconds(FLAGS_feature_cache_monitor_interval));
    }
  }

  void RemoveExpired() {
    std::lock_guard<std::mutex> lock(vm_mutex_);
    int version = cache_table_manager_.create_version();
    if (version >= 0) {
      cache_table_manager_[version].RemoveExpired();
      if (cache_table_manager_.freeze_version(version) != 0) {
        cache_table_manager_.drop_version(version);
        LOG(ERROR) << "freeze cache version error";
      }
    }
  }

 private:
  // DISALLOW_COPY_AND_ASSIGN(CacheManager);

  std::string name_;
  uint32_t cache_max_size_;
  st::VersionManager<TABLE> cache_table_manager_;
  std::mutex vm_mutex_;
};  // CacheManager

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_UTIL_CACHE_H_
