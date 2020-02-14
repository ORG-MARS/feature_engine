#ifndef FEATURE_ENGINE_PARSER_PARSER_COMMON_H_
#define FEATURE_ENGINE_PARSER_PARSER_COMMON_H_

#include <string>
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/feature/feature_conf_parser.h"
#include "feature_engine/reloader/data_manager.h"
#include "feature_engine/common/common_define.h"
#include "feature_engine/index/document.h"

namespace feature_engine {

using FeatureMap = google::protobuf::Map<int, feature_proto::Feature>;

extern const std::string kMissing;
extern const int kHashFeatureOffset;
#define HASH_FEATURE_ID(_id_) (_id_) + kHashFeatureOffset

// FIXME(lidongming):move to seperate header
#define CHECK_STATUS_RETURN(_status_) if (!_status_.ok()) { return _status_; }

// For GetDependFeature
#define FIRST_DEPEND 0
#define SECOND_DEPEND 1
#define THIRD_DEPEND 2
#define NOT_HASH_FEATURE false
#define HASH_FEATURE true

// For Method 'Match*'
#define MATCH_LENGTH true
#define NOT_MATCH_LENGTH false

// Typedefs of feature_proto types
#define KVListStringInt64 feature_proto::Feature::kVListStringInt64
#define KVListStringDouble feature_proto::Feature::kVListStringDouble
#define KVListString feature_proto::Feature::kVListString
#define KVListUint64 feature_proto::Feature::kVListUint64
#define KVString feature_proto::Feature::kVString
#define KVUint64 feature_proto::Feature::kVUint64
#define KVInt64 feature_proto::Feature::kVInt64
#define KVUint32 feature_proto::Feature::kVUint32
#define KVInt32 feature_proto::Feature::kVInt32
#define KV_KIND_NOT_SET feature_proto::Feature::KIND_NOT_SET

using commonlib::Status;

// Default status
extern const Status feature_parse_error_status;
extern const Status depend_not_found_status;
extern const Status invalid_type_status;
extern const Status invalid_param_status;
extern const Status invalid_doc_stat_status;
extern const Status not_found_doc_disc_item_status;

static inline Status CheckFeatureType(const feature_proto::Feature& feature,
                        const feature_proto::Feature::KindCase kind_case) {
  static const Status type_error_status(commonlib::error::FEATURE_PARSE_ERROR,
                                        "type error");
  if (kind_case != KV_KIND_NOT_SET) {
    if (feature.kind_case() != kind_case) {
      return type_error_status;
    }
  }
  return Status::OK();
}

#ifndef NDEBUG
template<typename T>
static inline void FeatureLog(const FeatureConf& feature_conf, const T& value) {
  LOG(INFO) << "set feature:" << feature_conf.id
            << " feature_name:" << feature_conf.name
            << " value:" << value;
}
#else
template<typename T>
static inline void FeatureLog(const FeatureConf& feature_conf, const T& value) {
}
#endif

#ifndef NDEBUG
template<typename T>
static inline void DocFeatureLog(Document* document,
    const FeatureConf& feature_conf, const T& value) {
  LOG(INFO) << "set feature:" << feature_conf.id
            << " feature_name:" << feature_conf.name
            << " docid:" << document->docid()
            << " value:" << value;
}
#else
template<typename T>
static inline void DocFeatureLog(Document* document,
    const FeatureConf& feature_conf, const T& value) {
}
#endif

static inline Status CheckDepends(const FeatureConf& feature_conf, int count) {
  if (feature_conf.depends.size() != count) {
    static const Status invalid_status(commonlib::error::FEATURE_PARSE_ERROR,
        "not enough depend features");
    return invalid_status;
  }
  return Status::OK();
}

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_PARSER_COMMON_H_
