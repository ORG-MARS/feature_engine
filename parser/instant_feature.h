// File   cross_feature.h
// Author lidongming
// Date   2018-10-10 09:56:07
// Brief

#ifndef FEATURE_ENGINE_PARSER_INSTANT_FEATURE_H_
#define FEATURE_ENGINE_PARSER_INSTANT_FEATURE_H_

#include <memory>
#include <string>
#include <vector>
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/parser/user_profile.h"
#include "feature_engine/index/document.h"
#include "feature_engine/ml-thrift/gen-cpp/feature_types.h"

namespace feature_engine {

using commonlib::Status;

class InstantFeature {
 public:
  InstantFeature();
  // WARNING:release feature_map manual, because feature_map will be cached
  // ~InstantFeature() {
  //   if (feature_map_ != NULL) {
  //     delete feature_map_;
  //     feature_map_ = NULL;
  //   }
  // }
  std::shared_ptr<feature_proto::FeaturesMap>& feature_map() {
    return feature_map_;
  }
  void set_feature_map(
      std::shared_ptr<feature_proto::FeaturesMap>& feature_map) {
    feature_map_ = feature_map;
  }
  std::shared_ptr<feature_proto::FeaturesMap>& cache_feature_map() {
    return cache_feature_map_;
  }
  void set_cache_feature_map(
      std::shared_ptr<feature_proto::FeaturesMap>& feature_map) {
    cache_feature_map_ = feature_map;
  }

  Status Parse(bool generate_textual_feature,
               UserProfile& user_profile,
               const std::shared_ptr<Document>& doc,
               const DocStat& doc_stat,
               const common_ml_thrift::DocInfo& docinfo);

 private:
  std::shared_ptr<feature_proto::FeaturesMap> feature_map_;
  std::shared_ptr<feature_proto::FeaturesMap> cache_feature_map_;
  // feature_proto::FeaturesMap* feature_map_;
};  // InstantFeature

}

#endif  // FEATURE_ENGINE_PARSER_INSTANT_FEATURE_H_
