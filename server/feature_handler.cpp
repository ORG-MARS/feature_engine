// File   feature_handler.cpp
// Author lidongming
// Date   2018-08-29 11:03:14
// Brief

#include <fstream>
#include <mutex>
#include <chrono>
#include <thread>
#include <iostream>
#include <atomic>
#include <fstream>
#include <string>
#include <utility>

#include "feature_engine/server/feature_handler.h"
#include "feature_engine/server/task.h"
#include "feature_engine/deps/commonlib/include/monitor.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"

#include "feature_engine/deps/commonlib/include/threadpool.h"
#include "feature_engine/deps/commonlib/include/env.h"
#include "feature_engine/deps/commonlib/include/env_time.h"

#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/index/index_entry.h"
#include "feature_engine/parser/parser_common.h"
#include "feature_engine/parser/parser_manager.h"
#include "feature_engine/parser/user_profile.h"
#include "feature_engine/parser/instant_feature.h"
#include "feature_engine/parser/cross_feature.h"
#include "feature_engine/feature/feature_conf_parser.h"
#include "feature_engine/deps/protobuf/include/google/protobuf/text_format.h"
#include "feature_engine/deps/boost/include/boost/threadpool.hpp"

namespace feature_engine {

using feature_thrift::FeatureRequest;
using feature_thrift::FeatureResponse;
using feature_thrift::SearchDocRequest;
using feature_thrift::SearchDocResponse;

using namespace commonlib;
using namespace commonlib::thread;

static const std::string kFeatureLatency = "feature_latency";
static const std::string kSampleLatency = "sample_latency";
static const std::string kActualFeatureLatency = "actual_feature_latency";
static const std::string kEmptyString = "";

std::atomic<int> kAtomicMissingSeq(0);
DEFINE_THREAD_LOCAL_BUFFER_ATOMIC(GetFeatures);
DEFINE_THREAD_LOCAL_BUFFER_ATOMIC(GenerateFeaturesParallel);
DEFINE_THREAD_LOCAL_BUFFER_ATOMIC(docid_missing);
DEFINE_THREAD_LOCAL_BUFFER_ATOMIC(docstat_missing);

#if 0
ThreadPool feature_threadpool(FLAGS_worker_thread_count, AFFINITY_DISABLE, 0);
// ThreadPool feature_threadpool(FLAGS_worker_thread_count, AFFINITY_AVERAGE, 0);
#endif

ThreadPool feature_threadpool(Env::Default(), "feature_threadpool",
                              std::thread::hardware_concurrency());

FeatureHandler::FeatureHandler() {
  Init();
}

FeatureHandler::~FeatureHandler() { }

void FeatureHandler::Init() {
  LOG(INFO) << "init feature handler";
  std::vector<std::string> business_list;
  commonlib::StringUtils::Split(FLAGS_business_list, ',', business_list);
  if (business_list.empty()) {
    LOG(FATAL) << "business list not set";
  } else {
    for (const std::string& v : business_list) {
      LOG(INFO) << "business:" << v;
    }
  }

  // 初始化missing队列，用于收集未命中的docid并异步更新
  missing_docid_manager_ = std::make_unique<
    MissingManager<CacheItem, MISSING_DOCID_QUEUE_CAPACITY>>(
        "missing_docid_manager",
        FLAGS_tls_queue_missing_item_set_size,
        FLAGS_tls_queue_missing_update_size,
        FLAGS_tls_queue_missing_update_interval);
  missing_docid_manager_->Start();

  // 初始化missing doc_stat 队列，用于收集未命中的docid并异步更新
  missing_docstat_manager_ = std::make_unique<
    MissingDocStatManager<CacheItem, MISSING_DOCSTAT_QUEUE_CAPACITY>>(
        "missing_docstat_manager",
        FLAGS_tls_queue_missing_item_set_size,
        FLAGS_tls_queue_missing_update_size,
        FLAGS_tls_queue_missing_update_interval);
  missing_docstat_manager_->Start();

  for (const std::string& v : business_list) {
    // 用于物料特征缓存，不同的business对应各自的缓存
    doc_caches_[v] = std::make_unique<
      CacheManager<FeatureCacheItem, FeatureCacheTable,
                   FEATURE_CACHE_QUEUE_CAPACITY>>(
          v + "_doc_cache_manager",
          FLAGS_tls_queue_item_set_size,
          FLAGS_tls_queue_update_size,
          FLAGS_tls_queue_update_interval,
          FLAGS_feature_cache_max_size);
    // 用于用户特征缓存，不同的business对应各自的缓存
    up_caches_[v] = std::make_unique<
      CacheManager<FeatureCacheItem, FeatureCacheTable,
                   FEATURE_CACHE_QUEUE_CAPACITY>>(
          v + "_up_cache_manager",
          FLAGS_tls_queue_item_set_size,
          FLAGS_tls_queue_update_size,
          FLAGS_tls_queue_update_interval,
          FLAGS_feature_cache_max_size);
  }
  for (auto& kv : doc_caches_) { kv.second->Start(); }
  for (auto& kv : up_caches_) { kv.second->Start(); }
}

// Thrift service 'Features'
void FeatureHandler::Features(FeatureResponse& response,
                              const FeatureRequest& request) {
  bool generate_textual_features = request.__isset.generate_textual_features
     ? request.generate_textual_features : false;

  LOG(INFO) << "process request rid:" << request.rid
            << " docs_size:" << request.docs.size()
            << " generate_textual_features:" << generate_textual_features;

  // Update qps monitor
  monitor::Monitor::inc("qpm", 1);

  // Timers
  TimeRecorder timer;
  timer.StartTimer(kFeatureLatency);

  timer.StartTimer(kActualFeatureLatency);

  GetFeatures(response, request);

  timer.StopTimer(kActualFeatureLatency);

  timer.StopTimer(kFeatureLatency);

  int64_t latency = timer.GetElapse(kFeatureLatency);

  if (generate_textual_features) {
    monitor::Monitor::avg("avg_gen_latency", latency);
    monitor::Monitor::max("max_gen_latency", latency);
  } else {
    monitor::Monitor::avg("avg_latency", latency);
    monitor::Monitor::max("max_latency", latency);
  }

  LOG(INFO) << "[FEATURE] rid:" << request.rid
            << " business:" << request.business
            << " generate_textual_features:" << generate_textual_features
            << " latency:" << timer.GetElapse(kFeatureLatency)
            << " feature_latency:" << timer.GetElapse(kActualFeatureLatency)
            << " request_time:" << timer.GetStartTime(kFeatureLatency)
            << " response_time:" << timer.GetStopTime(kFeatureLatency);

  if (latency > 100) { monitor::Monitor::inc("latency_100ms", 1); }
  else if (latency > 50) { monitor::Monitor::inc("latency_50ms", 1); }
  else if (latency > 30) { monitor::Monitor::inc("latency_30ms", 1); }
  else if (latency > 20) { monitor::Monitor::inc("latency_20ms", 1); }
  else { monitor::Monitor::inc("latency_less_20ms", 1); }
}

void FeatureHandler::GetFeatures(FeatureResponse& response,
                                 const FeatureRequest& request) {
  REGISTER_THREAD_LOCAL_BUFFER_SEQ(GetFeatures);
  size_t seq = THREAD_LOCAL_BUFFER_SEQ(GetFeatures);

  bool gen_hash_feature = false;
  bool gen_textual_feature = false;
  if (request.__isset.generate_textual_features
      && request.generate_textual_features) {
    gen_textual_feature = true;
  }

  // 存储TF特征
  std::vector<std::string>* serialized_features = NULL;
  // 存储LR特征
  std::vector<std::vector<int64_t>>* hash_features
    = &response.hash_features;
  std::vector<std::vector<std::string>>* textual_features
    = &response.textual_features;
  // 存储ALGO_TF特征
  std::vector<std::map<int32_t,  feature_thrift::FeatureInfo>>* features
    = &response.features;

  // LR获取特征哈希
  if (request.feature_type == feature_thrift::FeatureType::LR_FEATURE) {
    gen_hash_feature = true;
  } else if (request.feature_type == feature_thrift::FeatureType::TF_FEATURE) {
    serialized_features = &response.serialized_features;
  }

  UserProfile user_profile;
  user_profile.set_feature_request(&request);
  user_profile.set_common_user_info(&request.user_info);

  // 业务类型，比如头条、垂直频道、视频等
  // TODO(lidongming):make enum to avoid comparing string
  const std::string& business = request.business;

  // 用户特征是否命中缓存
  const std::string& uid = request.user_info.uid;

  // 根据业务字段获取对应的缓存
  FeatureCacheManagerType* up_cache = get_up_cache(business);
  bool hit_cache = false;
  if (FLAGS_enable_up_feature_cache && up_cache != NULL) {
    FeatureCacheItem cached_up;
    if (up_cache->GetCacheItem(uid, &cached_up)) {
      if (!cached_up.Expired()) {  // 缓存中的up数据未过期
        user_profile.set_cache_feature_map(cached_up.feature_map);
        hit_cache = true;
      }
    }
  }

  // 用户特征未命中缓存或已过期，重新生成用户特征
  Status status = user_profile.Parse(gen_hash_feature, gen_textual_feature);
  if (!status.ok()) {
    LOG(WARNING) << "parse user profile error rid:" << request.rid;
    return;
  }

  // 更新用户特征缓存
  if (FLAGS_enable_up_feature_cache && !hit_cache && up_cache) {
    FeatureCacheItem cache_item;
    cache_item.id = uid;
    cache_item.update_time = TimeUtils::GetCurrentTime();
    cache_item.expire_seconds = FLAGS_up_feature_cache_expire_seconds;
    cache_item.feature_map = user_profile.feature_map();
    up_cache->Push(seq, cache_item);
  }

  if (request.feature_type == feature_thrift::FeatureType::TF_FEATURE) {
    response.__isset.serialized_features = true;
  } else if (request.feature_type == feature_thrift::FeatureType::LR_FEATURE) {
    response.__isset.hash_features = true;
    if (gen_textual_feature) {
      response.__isset.textual_features = true;
    }
  } else if (request.feature_type == feature_thrift::FeatureType::ALGO_TF_FEATURE) {
    response.__isset.features = true;
  }
  GenerateFeatures(gen_textual_feature, request, user_profile,
      hash_features, textual_features, serialized_features, features);
}

void FeatureHandler::GenerateFeatures(bool gen_textual_feature,
    const FeatureRequest& request, UserProfile& user_profile,
    std::vector<std::vector<int64_t>>* hash_features,
    std::vector<std::vector<std::string>>* textual_features,
    std::vector<std::string>* serialized_features, 
    std::vector<std::map<int32_t, feature_thrift::FeatureInfo>>* features) {
  // TLS
  REGISTER_THREAD_LOCAL_BUFFER_SEQ(docid_missing);
  REGISTER_THREAD_LOCAL_BUFFER_SEQ(docstat_missing);

  size_t missing_docid_seq = THREAD_LOCAL_BUFFER_SEQ(docid_missing);
  size_t missing_docstat_seq = THREAD_LOCAL_BUFFER_SEQ(docstat_missing);

  // Parsers
  ParserManager& parser_manager = ParserManager::GetInstance();

  int docs_count = request.docs.size();
  hash_features->resize(docs_count);
  if (gen_textual_feature) {
    textual_features->resize(docs_count);
  }

  // 物料正排索引查询入口
  IndexEntry& index_entry = IndexEntry::GetInstance();

  // 获取此次请求中所有物料数据
  std::vector<std::shared_ptr<Document>> docs(docs_count, nullptr);

  thread_local size_t missing_seq = ++kAtomicMissingSeq;

  MapStringIntAutoLock& updating_doc_comments_tl
      = index_entry.updating_doc_comments(missing_seq);

  std::vector<uint32_t> missing_docs_seq;
  std::vector<uint32_t> missing_doc_stats_seq;

  // 缓存透传过来的评论数
  std::map<std::string, int32_t> map_doc_comments;

  // 文章的统计信息，比如评论数量、曝光数量等，默认为invalid
  std::vector<DocStat> doc_stats(docs_count);

  int get_doc_stat_success_count = 0;
  for (size_t i = 0; i < docs_count; i++) {
    const common_ml_thrift::DocInfo& cur_doc = request.docs[i];
    const std::string& docid = cur_doc.docid;

    // 缓存透传过来的评论数
    map_doc_comments.insert(std::make_pair(docid, cur_doc.comment_num));

    const std::shared_ptr<Document>& d = index_entry.GetDocument(docid);
    if (d == nullptr) {
      // 异步更新未存储的文章
      if (FLAGS_update_missing) {
        missing_docs_seq.push_back(i);
      }
    }
    else {
      int get_doc_stat_ret = index_entry.GetDocStat(docid, &doc_stats[i]);
      if (get_doc_stat_ret == 0) {
        ++get_doc_stat_success_count;
      }
      else {
        // 异步更新未存储的文章统计信息
        if (FLAGS_update_missing) {
          missing_doc_stats_seq.push_back(i);
        }
      }
    }
    docs[i] = d;
  }
  monitor::Monitor::avg("get_doc_stat_success_count"
                        , get_doc_stat_success_count);
  monitor::Monitor::max("max_get_doc_stat_success_count"
                        , get_doc_stat_success_count);

  // 缓存透传过来的评论数
  updating_doc_comments_tl.Add(map_doc_comments);

  if (FLAGS_debug_get_doc_property_comment) {
    std::stringstream ss;
    ss << "[GenerateFeatures] debug_get_doc_property_comment:";
    for (const auto& item : map_doc_comments) {
      const auto& doc_id = item.first;
      const auto& comment_num = item.second;
      ss << "docid_" << doc_id << "_commentNum_" << comment_num << ",";
    }
    LOG(INFO) << ss.str();
  }

  if (FLAGS_update_missing) {
    for (size_t i : missing_docs_seq) {
      const std::string& docid = request.docs[i].docid;
      missing_docid_manager_->Push(missing_docid_seq, docid);
    }

    for (size_t i : missing_doc_stats_seq) {
      const std::string& docid = request.docs[i].docid;
      missing_docstat_manager_->Push(missing_docstat_seq, docid);
    }
  }

  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();

  std::map<int, FeatureConf>& feature_conf_map
    = feature_conf_parser.feature_conf_map();
  std::vector<FeatureConf> request_feature_conf_vector;
  for (const int feature_id : request.feature_ids) {
    if (feature_conf_map.find(feature_id) == feature_conf_map.end()) {
      LOG(WARNING) << "rid:" << request.rid << " feature_id not found "
                   << "feature_id:" << feature_id;
      request_feature_conf_vector.emplace_back(FeatureConf());
    } else {
      request_feature_conf_vector.emplace_back(feature_conf_map[feature_id]);
    }
  }

  const std::string& business = request.business;

  int task_count = 1;
  // 构造异步task并行抽取特征
  // 若文章数量过小则不再通过线程池并行抽取
  if (docs_count > (1 + FLAGS_tasks_factor) * FLAGS_docs_count_per_task) {
    task_count = (int)std::ceil(docs_count * 1.0 / FLAGS_docs_count_per_task);
    if (task_count > FLAGS_max_tasks_count) {
      LOG(WARNING) << "invalid task_count rid:" << request.rid
                   << " task_count:" << task_count;
      return;
    }
  }

  // 根据业务字段获取特征缓存
  FeatureCacheManagerType* doc_cache = get_doc_cache(business);
  if (doc_cache == NULL) {
    LOG(INFO) << "cache not found business:" << business
              << " rid:" << request.rid;
  }

  TaskTracker task_tracker(task_count);

  for (size_t task_num = 0; task_num < task_count; task_num++) {
    int start = task_num * FLAGS_docs_count_per_task;
    int end = (task_num + 1) * FLAGS_docs_count_per_task;
    if (start >= docs.size()) { start = docs.size(); }
    if (end > docs.size()) { end = docs.size(); }

    FeatureTask task;
    task.rid = request.rid;
    task.tracker = &task_tracker;
    task.start = start;  // start、end为此task所处理的文章索引区间
    task.end = end;
    task.gen_textual_feature = gen_textual_feature;  // 是否生成特征明文
    task.request = &request;  // FeatureRequest
    task.user_profile = &user_profile;
    task.docs = &docs;  // 候选文章集合
    task.doc_stats = &doc_stats;  // 文章统计信息集合
    task.request_feature_conf_vector = &request_feature_conf_vector; // 特征列表
    task.hash_features = hash_features;
    task.textual_features = textual_features;

    task.feature_type = request.feature_type;
    if (task.feature_type == feature_thrift::FeatureType::type::TF_FEATURE) {
      if (serialized_features) {
        serialized_features->resize(docs_count, "");
      }
    }
    task.serialized_features = serialized_features;

    if (task.feature_type == feature_thrift::FeatureType::type::ALGO_TF_FEATURE) {
      features->resize(docs_count);
      task.features = features;
    }

    // 生成特征明文时不查询cache，cache中的大部分特征没有明文
    if (!gen_textual_feature) {
      task.doc_cache = doc_cache;
    }

    feature_threadpool.Schedule([=]() {
          GenerateFeaturesParallel(task);
          task.tracker->done();
        });
  }

  std::unique_lock<std::mutex> l(task_tracker.done_lock);
  if (!task_tracker.done_flag) {
    task_tracker.done_cv.wait(l);
  }
}

void FeatureHandler::GenerateFeaturesParallel(FeatureTask task) {
  REGISTER_THREAD_LOCAL_BUFFER_SEQ(GenerateFeaturesParallel);
  size_t seq = THREAD_LOCAL_BUFFER_SEQ(GenerateFeaturesParallel);

  int start = task.start;
  int end = task.end;
  if (start >= end) {
    return;
  }

  std::vector<std::shared_ptr<Document>>* docs = task.docs;
  std::vector<DocStat>* doc_stats = task.doc_stats;
  bool gen_textual_feature = task.gen_textual_feature;
  const FeatureRequest& request = *task.request;
  UserProfile& user_profile = *task.user_profile;
  std::vector<FeatureConf>& request_feature_conf_vector
    = *task.request_feature_conf_vector;
  std::vector<std::vector<int64_t>>* hash_features = task.hash_features;
  std::vector<std::vector<std::string>>* textual_features = task.textual_features;
  std::vector<std::map<int32_t, feature_thrift::FeatureInfo>>* features = task.features;
  FeatureCacheManagerType* doc_cache = task.doc_cache;

  std::vector<std::string>* serialized_features = task.serialized_features;

  std::vector<FeatureCacheItem> feature_cache_items;

  for (int i = start; i < end; i++) {
    const std::shared_ptr<Document>& d = (*docs)[i];
    if (d == nullptr) {
      continue;
    }

    // 实时特征
    InstantFeature instant_feature;
    // Instant文章特征可以缓存
    // 1.Check If Doc Exists In Cache
    const std::string& docid = d->docid();
    bool hit_cache = false;
    if (FLAGS_enable_doc_feature_cache && doc_cache != NULL) {
      FeatureCacheItem cache_doc;
      if (doc_cache->GetCacheItem(docid, &cache_doc)) {
        if (!cache_doc.Expired()) {
          instant_feature.set_cache_feature_map(cache_doc.feature_map);
          hit_cache = true;
        }
      }
    }

    const common_ml_thrift::DocInfo& docinfo = request.docs[i];
    Status status = instant_feature.Parse(gen_textual_feature, user_profile,
                                          d, (*doc_stats)[i], docinfo);
    if (!status.ok()) {
      LOG(WARNING) << "parse instant features error";
      continue;
    }

    // 更新instant特征缓存
    if (FLAGS_enable_doc_feature_cache && !hit_cache && doc_cache) {
      FeatureCacheItem cache_item;
      cache_item.id = d->docid();
      cache_item.update_time = TimeUtils::GetCurrentTime();
      cache_item.expire_seconds = FLAGS_doc_feature_cache_expire_seconds;
      cache_item.feature_map = instant_feature.feature_map();
      doc_cache->Push(seq, std::move(cache_item));
    }

    // 交叉特征
    CrossFeature cross_feature;
    status = cross_feature.Parse(gen_textual_feature, user_profile, d,
                                 instant_feature, (*doc_stats)[i]);
    if (!status.ok()) {
      LOG(WARNING) << "parse cross features error rid:" << request.rid;
      continue;
    }
    const auto& cross_feature_map = cross_feature.feature_map();
    if (cross_feature_map == nullptr) {
      LOG(WARNING) << "cross feature_map is null rid:" << request.rid;
      continue;
    }

    std::vector<int64_t>& doc_hash_features = (*hash_features)[i];
    std::vector<std::string>& doc_textual_features = (*textual_features)[i];

    // 用户特征
    const auto& up_feature_map = user_profile.feature_map();
    const auto& cache_up_feature_map = user_profile.cache_feature_map();
    // 基础物料特征
    const auto& doc_feature_map = d->feature_map();
    // 实时特征(主要是为output类型的物料特征及instant类型的特征，
    //   具体在conf/feature/features.xml中定义)
    const auto& instant_feature_map = instant_feature.feature_map();
    const auto& cache_instant_feature_map = instant_feature.cache_feature_map();

    // 存储本次请求所需的lr特征
    feature_proto::FeaturesMap features_map;
    // 存储本次请求所需的tf特征
    std::string serialized_feature_value;
    int ret = 0;

    // 遍历请求中的特征id列表
    for (size_t j = 0; j < request.feature_ids.size(); j++) {
      const int feature_id = request.feature_ids[j];
      const FeatureConf& feature_conf = request_feature_conf_vector[j];
      feature_proto::FeaturesMap* feature_map = NULL;
      // 根据特征类型获取对应的feature_map，特征类型需要在xml中仔细定义
      // FIXME(lidongming):判断特征是否已经cache
      switch (feature_conf.conf_type) {
        case UP_FEAUTRE_CONF:
          if (feature_conf.enable_cache && cache_up_feature_map) {
            feature_map = cache_up_feature_map.get();
          } else {
            feature_map = up_feature_map.get();
          }
          break;
        case DOC_FEAUTRE_CONF:
          if (feature_conf.enable_cache && cache_instant_feature_map) {
            feature_map = cache_instant_feature_map.get();
          } else {
            feature_map = doc_feature_map.get();
          }
          break;
        case INSTANT_FEAUTRE_CONF:
          if (feature_conf.enable_cache && cache_instant_feature_map) {
            feature_map = cache_instant_feature_map.get();
          } else {
            feature_map = instant_feature_map.get();
          }
          break;
        case CROSS_FEAUTRE_CONF:
          feature_map = cross_feature_map.get();
          break;
        default:
          feature_map = instant_feature_map.get();
          break;
      }

      // 从对应的feature_map中获取feature_id对应的特征值并存储
      if (task.feature_type == feature_thrift::FeatureType::TF_FEATURE) {
        const feature_proto::Feature* feature = NULL;
        const auto& fit = feature_map->f().find(feature_id);
        if (fit != feature_map->f().end()) {
          feature = &fit->second;
          auto* features = features_map.mutable_f();
          (*features)[feature_id] = fit->second;
        }
        // else {
          // monitor::Monitor::inc("miss_tf_feature", 1);
        // }
      } else if (task.feature_type == feature_thrift::FeatureType::LR_FEATURE) {
        ret = ProcessLRFeatures(feature_conf, gen_textual_feature, feature_map,
                                &doc_hash_features, &doc_textual_features);
        // if (ret != 0) {
          // monitor::Monitor::inc("miss_lr_feature", 1);
        // }
      } else if (task.feature_type == feature_thrift::FeatureType::ALGO_TF_FEATURE) {
        std::map<int32_t, feature_thrift::FeatureInfo>& algo_tf_features = (*features)[i];
        ret = ProcessAlgoTFFeatures(feature_conf, gen_textual_feature, feature_map,
                                &algo_tf_features);
        // if (ret != 0) {
          // monitor::Monitor::inc("miss_algotf_feature", 1);
        // }
      }
    }  // end for feature_ids in request

    // tf特征需要序列化成string
    if (task.feature_type == feature_thrift::FeatureType::TF_FEATURE) {
      features_map.SerializeToString(&serialized_feature_value);
      (*serialized_features)[i] = std::move(serialized_feature_value);
      if (FLAGS_print_features) {
        std::string v;
        google::protobuf::TextFormat::PrintToString(features_map, &v);
        LOG(INFO) << "rid:" << request.rid << " docid:" << docid
                  << " features:" << v;
      }
    }
  }
}

int FeatureHandler::ProcessAlgoTFFeatures(const FeatureConf& feature_conf,
    bool gen_textual_feature,
    feature_proto::FeaturesMap* feature_map,
    std::map<int32_t, feature_thrift::FeatureInfo>* algo_tf_features) {
  int32_t feature_id = feature_conf.id;
  //
  feature_thrift::FeatureInfo& feature_info = (*algo_tf_features)[feature_id]; 
  feature_info.__set_feature_id(feature_id);
  // 获取特征hash
  uint64_t hash_feature_id = feature_id + kHashFeatureOffset;
  auto& features = feature_map->f();
  auto it = features.find(hash_feature_id);
  const feature_proto::Feature* hfeature = NULL;
  if (it != features.end()) {
    hfeature = &it->second;
    if (FLAGS_debug) {
      std::string content ;
      google::protobuf::TextFormat::PrintToString(*hfeature, &content);
      LOG(INFO) << "feature_id:"<< feature_id << ",feature:" << content;
    }
  }
  const feature_proto::Feature* feature = NULL;
  it = features.find(feature_id);
  if (it != features.end()) {
    feature = &it->second;
    if (FLAGS_debug) {
      std::string content ;
      google::protobuf::TextFormat::PrintToString(*feature, &content);
      LOG(INFO) << "feature_id:"<< feature_id << ",feature:" << content;
    }
  }
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();
  //hash
  if (hfeature != NULL) {
    if (hfeature->kind_case() == KVUint64) {
      feature_info.__isset.hash_features = true;
      feature_info.hash_features.emplace_back(hfeature->v_uint64());
    } else if (hfeature->kind_case() == KVListUint64) {
      feature_info.__isset.hash_features = true;
      feature_info.hash_features.resize(hfeature->v_list_uint64().k_size());
      for (int i = 0; i < hfeature->v_list_uint64().k_size(); i++) {
        feature_info.hash_features[i] = hfeature->v_list_uint64().k(i);
      }
    } 
  }
  //
  if (feature != NULL) { 
    if (feature->kind_case() == KVInt32) {
      feature_info.__isset.number_features = true;
      feature_info.number_features.emplace_back(feature->v_int32());
    } else if (feature->kind_case() == KVUint32) {
      feature_info.__isset.number_features = true;
      feature_info.number_features.emplace_back(feature->v_uint32());
    } else if (feature->kind_case() == KVListStringDouble) {
      feature_info.__isset.hash_features = true;
      feature_info.__isset.scores = true;
      feature_info.hash_features.resize(feature->v_list_string_double().k_size());
      feature_info.scores.resize(feature->v_list_string_double().k_size());
      for (int i = 0; i < feature->v_list_string_double().k_size(); i++) {
        feature_info.hash_features[i] = MAKE_HASH(feature->v_list_string_double().k(i));
        feature_info.scores[i] = feature->v_list_string_double().w(i);
      }
    } else if (feature->kind_case() == KVInt64) {
      feature_info.__isset.number_features = true;
      feature_info.number_features.emplace_back(feature->v_int64());
    } else if (feature->kind_case() == KVUint64) {
      feature_info.__isset.number_features = true;
      feature_info.number_features.emplace_back(feature->v_uint64());
    //} else if (feature->kind_case() == KVString) {
      //feature_info.__isset.string_features = true;
      //feature_info.string_features.emplace_back(feature->v_string());
    //} else if (feature->kind_case() == KVListString) {
      //feature_info.__isset.string_features = true;
      //feature_info.string_features.resize(feature->v_list_string().k_size());
      //for (int i = 0; i < feature->v_list_string().k_size(); i++) {
      //  feature_info.string_features[i] = feature->v_list_string().k(i);
      //}
    }
  }
  return 0;
}


int FeatureHandler::ProcessLRFeatures(const FeatureConf& feature_conf,
    bool gen_textual_feature,
    feature_proto::FeaturesMap* feature_map,
    std::vector<int64_t>* doc_hash_features,
    std::vector<std::string>* doc_textual_features
    ) {
  int feature_id = feature_conf.id;
  // 获取特征hash
  uint64_t hash_feature_id = feature_id + kHashFeatureOffset;
  auto& features = feature_map->f();
  auto it = features.find(hash_feature_id);
  if (it == features.end()) {
    return -1;
  }
  const auto& hfeature = it->second;

  const feature_proto::Feature* feature = NULL;
  if (gen_textual_feature) {
    it = features.find(feature_id);
    if (it != features.end()) {
      feature = &it->second;
    }
  }

  std::string textual_feature;

  // feature id hash
  uint64_t id_hash = feature_conf.id_hash;
  uint64_t hash_feature = 0;
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();

  // 单值特征的哈希值类型为uint64
  if (hfeature.kind_case() == KVUint64) {
    hash_feature = GEN_HASH2(id_hash, hfeature.v_uint64());
    doc_hash_features->push_back(hash_feature);
    if (gen_textual_feature && feature != NULL) {  // 构造明文特征
      if (feature->kind_case() == KVString) {
        textual_feature = feature_conf_parser.GetFeatureName(feature_id)
          + "&" + feature->v_string();
        doc_textual_features->emplace_back(textual_feature);
#ifndef NDEBUG
        LOG(INFO) << "add textual feature:" << textual_feature;
#endif
      }
    }
  } else if (hfeature.kind_case() == KVListUint64) {
    // 多值特征的哈希值类型为uint64数组
    for (int i = 0; i < hfeature.v_list_uint64().k_size(); i++) {
      hash_feature = GEN_HASH2(id_hash, hfeature.v_list_uint64().k(i));
      doc_hash_features->push_back(hash_feature);
      if (gen_textual_feature && feature != NULL) {  // 构造明文特征
        if (i >= feature->v_list_string().k_size()) {
          continue;
        }
        if (feature->kind_case() == KVListString) {
          textual_feature = feature_conf_parser.GetFeatureName(feature_id)
            + "&" + feature->v_list_string().k(i);
          doc_textual_features->emplace_back(textual_feature);
#ifndef NDEBUG
          LOG(INFO) << "add textual feature:" << textual_feature;
#endif
        }
      }
    }
  }  // end if (hfeature.kind_case() == kVListUint64)
  return 0;
}

// 获取内存中文章的接口
void FeatureHandler::SearchDoc(SearchDocResponse& response, 
    const SearchDocRequest& request) {
  if (request.docid.empty() || request.feature_ids.empty()) {
    LOG(WARNING) << "in SearchDoc docid or feature_ids is empty rid:"
      << request.rid;
    return;
  }

  // 提前获取feature_id对应的profile类型，如user_profile和doc_profile
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();
  std::vector<FeatureSource> feature_source_vector;
  for (const int id : request.feature_ids) {
    feature_source_vector.push_back(feature_conf_parser.GetFeatureSource(id));
  }

  IndexEntry& index_entry = IndexEntry::GetInstance();
  const std::string& docid = request.docid;
  LOG(INFO) << "enter into SearchDoc, rid:" << request.rid << " docid:" << docid;
  const std::shared_ptr<Document>& d = index_entry.GetDocument(docid);
  if (d == nullptr) {
    LOG(INFO) << "in SearchDoc not found rid:" << request.rid\
      << " docid:" << docid;
    response.__set_is_exist(0);
    return;
  }
  response.__set_is_exist(1);

  std::shared_ptr<feature_proto::FeaturesMap>& doc_feature_map = d->feature_map();
  response.__set_expire_time(d->expire_time());
  if (doc_feature_map == nullptr) {
    LOG(INFO) << "in SearchDoc feature_map is null rid:" << request.rid
      << " docid:" << docid;
    return;
  }
  feature_proto::FeaturesMap features_map;
  std::map<int32_t, std::string> result_map;
  for (size_t i = 0; i < request.feature_ids.size(); i++) {
    int feature_id = request.feature_ids[i];
    DLOG(INFO) << "rid:" << request.rid << " feature_id:" << feature_id;
    feature_proto::FeaturesMap* feature_map = NULL;
    if (feature_source_vector[i] == DOC_PROFILE) {
      feature_map = doc_feature_map.get();
    } else {
      continue;
  }
    if (feature_map == NULL) {  // OTHER_FEATURE
      continue;
    }
    const auto& it = feature_map->f().find(feature_id);
    if (it != feature_map->f().end()) {
      auto* features = features_map.mutable_f();
      (*features)[feature_id] = it->second;
      std::string v;
      google::protobuf::TextFormat::PrintToString(it->second, &v);
      DLOG(INFO) << "featrue_id:" << feature_id << " find,value" << v;
      result_map[feature_id] = v;
    }
  }
  response.__set_feature_map(result_map);
}

void FeatureHandler::GetDocProperty(
    doc_property_thrift::GetDocPropertyResponse& ret,
    const doc_property_thrift::GetDocPropertyRequest& request) {
  // Timers
  TimeRecorder timer;
  static const std::string kGetDocPropertyLatency = "GetDocProperty";
  timer.StartTimer(kGetDocPropertyLatency);

  // Update qps monitor
  monitor::Monitor::inc("GetDocProperty_qpm", 1);

  int comments_num_greater_than_0 = 0;
  if (request.docids.size() == 0) {
    ret.__set_err_code(-1);
    ret.__set_msg("request.docids.size() == 0");
  } else {
    // 获取评论数
    static const std::string kCommentCount("commentCount");
    if (request.property_names.size() == 0
        || request.property_names.count(kCommentCount) > 0) {
      // 物料正排索引查询入口
      IndexEntry& index_entry = IndexEntry::GetInstance();
      for (const auto& docid : request.docids) {
        int32_t doc_comment = 0;
        index_entry.GetDocComment(docid, &doc_comment);
        if (doc_comment > 0) {
          ++comments_num_greater_than_0;
        }

        doc_property_thrift::MultiType val;
        val.__set_i64_val(doc_comment);
        ret.properties[docid].insert(
            std::make_pair(kCommentCount, std::move(val)));
      }
      ret.__isset.properties = true;
    }
    ret.__set_err_code(0);
  }
  timer.StopTimer(kGetDocPropertyLatency);
  int64_t latency = timer.GetElapse(kGetDocPropertyLatency);

  monitor::Monitor::avg("GetDocProperty_avg_latency", latency);
  monitor::Monitor::max("GetDocProperty_max_latency", latency);

  std::stringstream ss;
  ss << "[GetDocProperty] rid:" << request.rid
      << " err_code:" << ret.err_code
      << " msg:" << ret.msg
      << " doc_size:" << request.docids.size()
      << " comments_num_gt0:" << comments_num_greater_than_0
      << " latency:" << latency
      << " request_time:" << timer.GetStartTime(kGetDocPropertyLatency)
      << " response_time:" << timer.GetStopTime(kGetDocPropertyLatency);

  if (FLAGS_debug_get_doc_property_comment) {
    ss << " debug_return_thrift:" << ret;
  }

  if (ret.__isset.err_code && ret.err_code == 0) {
    LOG(INFO) << ss.str();
  } else {
    LOG(WARNING) << ss.str();
  }
}

}  // namespace feature_engine
