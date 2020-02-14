// File   feature_handler.h
// Author lidongming
// Date   2018-09-11 01:28:05
// Brief

#ifndef FEATURE_ENGINE_SERVER_SERVER_FEATURE_HANDLER_H_
#define FEATURE_ENGINE_SERVER_SERVER_FEATURE_HANDLER_H_

#include <map>
#include <vector>
#include <memory>
#include <atomic>
#include <condition_variable>

#include "feature_engine/ml-thrift/gen-cpp/FeatureService.h"
#include "feature_engine/ml-thrift/gen-cpp/feature_types.h"
#include "feature_engine/ml-thrift/gen-cpp/DocPropertyService.h"
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/index/document.h"
#include "feature_engine/parser/user_profile.h"
// #include "feature_engine/parser/feature_cache.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/server/task.h"
#include "feature_engine/util/cache.h"
#include "feature_engine/index/missing_manager.h"

namespace feature_engine {

struct TaskTracker {
  TaskTracker(int task_count) {
    count.store(task_count);
  }
  std::atomic_int_fast32_t count;
  std::mutex done_lock;
  std::condition_variable done_cv;
  bool done_flag = false;

  void done() {
    if (count.fetch_sub(1) == 1) {
      // mutex_lock l(done_lock);
      std::unique_lock<std::mutex> l(done_lock);
      done_flag = true;
      done_cv.notify_all();
    }
  }
};

#define MISSING_DOCID_QUEUE_CAPACITY 10000
#define MISSING_DOCSTAT_QUEUE_CAPACITY 10000
#define FEATURE_CACHE_QUEUE_CAPACITY 100

using FeatureCacheManagerType = CacheManager<FeatureCacheItem,
                                             FeatureCacheTable,
                                             FEATURE_CACHE_QUEUE_CAPACITY>;
using MissingManagerType = MissingManager<CacheItem,
                                          MISSING_DOCID_QUEUE_CAPACITY>;
using MissingDocStatManagerType = MissingDocStatManager<CacheItem,
                                    MISSING_DOCSTAT_QUEUE_CAPACITY>;

struct FeatureTask {
  std::string rid;
  int start = 0;
  int end = 0;
  bool gen_textual_feature = false;
  const feature_thrift::FeatureRequest* request = NULL;
  UserProfile* user_profile = NULL;
  std::vector<std::shared_ptr<Document>>* docs = NULL;
  std::vector<DocStat>* doc_stats = NULL;
  std::vector<FeatureConf>* request_feature_conf_vector = NULL;
  std::vector<std::vector<int64_t>>* hash_features = NULL;
  std::vector<std::vector<std::string>>* textual_features = NULL;
  std::vector<std::string>* serialized_features = NULL;
  std::vector<std::map<int32_t, feature_thrift::FeatureInfo>>* features = NULL;
  feature_thrift::FeatureType::type feature_type;
  // FeatureCache* doc_cache = NULL;
  FeatureCacheManagerType* doc_cache = NULL;
  TaskTracker* tracker;
};

class FeatureHandler :
  virtual public feature_thrift::FeatureServiceIf,
  virtual public doc_property_thrift::DocPropertyServiceIf {
 public:
  explicit FeatureHandler();
  ~FeatureHandler();	

  void Init();
  void Features(feature_thrift::FeatureResponse& response,
      const feature_thrift::FeatureRequest& request);

  static int ProcessLRFeatures(const FeatureConf& feature_conf,
      bool gen_textual_feature,
      feature_proto::FeaturesMap* feature_map,
      std::vector<int64_t>* doc_hash_features,
      std::vector<std::string>* doc_textual_features);

  static int ProcessAlgoTFFeatures(const FeatureConf& feature_conf,
      bool gen_textual_feature,
      feature_proto::FeaturesMap* feature_map,
      std::map<int32_t, feature_thrift::FeatureInfo>* features);

  void SearchDoc(feature_thrift::SearchDocResponse& response,
      const feature_thrift::SearchDocRequest& request);

  void GetFeatures(feature_thrift::FeatureResponse& response,
      const feature_thrift::FeatureRequest& request);

  void GetDocumentFeatures(const feature_thrift::FeatureRequest& request,
      UserProfile& user_profile,
      std::vector<std::string>* serialized_features);

  void GetUserFeatures(bool generate_hash_features,
      bool generate_textual_feature,
      const feature_thrift::FeatureRequest& request,
      UserProfile* user_profile
      // , std::vector<int64_t>* hash_features,
      // std::vector<std::string>* textual_features
      );

  void GenerateFeatures(bool generate_textual_feature,
      const feature_thrift::FeatureRequest& request,
      UserProfile& user_profile,
      std::vector<std::vector<int64_t>>* hash_features,
      std::vector<std::vector<std::string>>* textual_features, 
      std::vector<std::string>* serialized_features, 
      std::vector<std::map<int32_t, feature_thrift::FeatureInfo>>* features);

  // static void GenerateFeaturesParallel(FeatureTask& task);
  static void GenerateFeaturesParallel(FeatureTask task);

  FeatureCacheManagerType* get_up_cache(const std::string& business) {
#if 0
    if (business == "toutiao") {
      return headline_up_cache_.get();
    } else if (business == "channels") {
      return channels_up_cache_.get();
    } else if (business == "video") {
      return video_up_cache_.get();
    } else {
      return NULL;
    }
#endif
    const auto it = up_caches_.find(business);
    if (it != up_caches_.end()) {
      return it->second.get();
    }
    return NULL;
  }

  FeatureCacheManagerType* get_doc_cache(const std::string& business) {
#if 0
    if (business == "headline") {
      return headline_doc_cache_.get();
    } else if (business == "channels") {
      return channels_doc_cache_.get();
    } else if (business == "video") {
      return video_doc_cache_.get();
    } else {
      return NULL;
    }
#endif
    const auto it = doc_caches_.find(business);
    if (it != doc_caches_.end()) {
      return it->second.get();
    }
    return NULL;
  }

  virtual void GetDocProperty(
      doc_property_thrift::GetDocPropertyResponse& _return,
      const doc_property_thrift::GetDocPropertyRequest& request) override;

 private:
  std::unique_ptr<MissingManagerType> missing_docid_manager_;
  std::unique_ptr<MissingDocStatManagerType> missing_docstat_manager_;

  // Caches
  std::map<std::string, std::unique_ptr<FeatureCacheManagerType>> doc_caches_;
  std::map<std::string, std::unique_ptr<FeatureCacheManagerType>> up_caches_;
};  // FeatureHandler

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_SERVER_SERVER_FEATURE_HANDLER_H_
