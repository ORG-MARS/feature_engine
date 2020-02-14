// File   document.h
// Author lidongming
// Date   2018-09-05 00:33:21
// Brief

#ifndef FEATURE_ENGINE_PARSER_DOCUMENT_H_
#define FEATURE_ENGINE_PARSER_DOCUMENT_H_

#include <memory>
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

namespace feature_engine {

using commonlib::Status;

// 物料统计特征
struct DocStat {
  bool valid = false;

  // int read_num = 0;
  // int post_num = 0;
  // int ding_num = 0;
  // int exp_num = 0;
  // int browse_num = 0;
  // int click_num = 0;
  // int read_duration = 0;
  // int read_progress = 0;
  // int avg_progress = 0;
  // int avg_duration = 0;
  std::unordered_map<std::string, int> stat_items;

  int expire_time = FLAGS_doc_stat_expire_seconds;  // 3 minutes default
  int priority = 0;  // Refresh pririty
  int64_t timestamp = 0;  // milliseconds
  std::string docid;

  // explicit DocStat(int64_t tm) : timestamp(tm) {  }
  int Parse(const std::string& json_str);
  void Dump() const;
  inline bool IsExpired(int64_t current_time) const {
    return (current_time - timestamp) > (expire_time * 1000 * (1 + priority));
  }
  inline void DecreasePriority() {
    if (priority >= FLAGS_max_doc_stat_priority) {
      return;
    } else {
      ++priority;
    }
  }
  inline void ResetPriority() {
    priority = 0;
  }
  inline void set_valid(bool v) {
    valid = v;
  }
  inline int get_value(const std::string& name, int* value) const {
    const auto it = stat_items.find(name);
    if (it == stat_items.end()) {
      return -1;
    } else {
      *value = it->second;
      return 0;
    }
  }
};

class Document {
 public:
  Document();

  Status ParseFromJson(const std::string& json_value);

  void Init();
  Status IsValid();

  void CheckFeatures();

  std::shared_ptr<feature_proto::FeaturesMap>& feature_map() {
    return feature_map_;
  }

  std::string docid() {
    if (docid_.empty()) {
      set_docid();
    }
    return docid_;
  }
  void set_docid(const std::string& docid) {
    docid_ = docid;
  }

  void set_docid() {
    if (feature_map_ == nullptr) {
      return;
    }
    const auto& feature_map = feature_map_->f();
    const auto it = feature_map.find(kFeatureDocID);
    if (it != feature_map.end()) {
      docid_ = it->second.v_string();
    }
  }

  void set_feature_map(
      std::shared_ptr<feature_proto::FeaturesMap>& feature_map) {
    feature_map_ = feature_map;
    set_docid();
    set_expire_time();
  }

  void set_feature_map(feature_proto::FeaturesMap&& feature_map) {
    *feature_map_ = feature_map;
  }

  int64_t expire_time() {
    if (expire_time_ <= 0) {
      set_expire_time();
    }
    return expire_time_;
  }

  void set_expire_time(int64_t expire) {
    expire_time_ = expire;
  }

  void set_expire_time() {
    if (feature_map_ == nullptr) {
      return;
    }
    const auto& feature_map = feature_map_->f();
    const auto it = feature_map.find(kFeatureExpireTime);
    if (it != feature_map.end()) {
      expire_time_ = it->second.v_int64();
    }
  }

  void set_doc_stat(const DocStat& doc_stat) {
    doc_stat_ = doc_stat;
  }

  const DocStat& doc_stat() {
    return doc_stat_;
  }
 private:
  std::shared_ptr<feature_proto::FeaturesMap> feature_map_;
  std::string docid_;
  int64_t expire_time_;  // seconds
  DocStat doc_stat_;
};  // Document

class DocComment {
 public:
  int expire_time = FLAGS_doc_stat_expire_seconds;  // 3 minutes default;
  int priority = 0;  // Refresh pririty
  int64_t timestamp = 0;  // milliseconds

  int32_t comment_num = 0;
};

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_DOCUMENT_H_
