// File   cross_feature.h
// Author lidongming
// Date   2018-12-25 18:29:48
// Brief

#ifndef FEATURE_ENGINE_PARSER_CROSS_FEATURE_H_
#define FEATURE_ENGINE_PARSER_CROSS_FEATURE_H_

#include <memory>
#include <string>
#include <vector>
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/parser/user_profile.h"
#include "feature_engine/parser/instant_feature.h"
#include "feature_engine/index/document.h"
#include "feature_engine/ml-thrift/gen-cpp/feature_types.h"

namespace feature_engine {

using commonlib::Status;

class CrossFeature {
 public:
  CrossFeature();
  std::shared_ptr<feature_proto::FeaturesMap>& feature_map() {
    return feature_map_;
  }

  Status Parse(bool gen_textual_feature, UserProfile& user_profile,
               const std::shared_ptr<Document>& doc,
               InstantFeature& instant_feature,
               const DocStat& doc_stat);
 private:
  std::shared_ptr<feature_proto::FeaturesMap> feature_map_;
  // feature_proto::FeaturesMap* feature_map_;
};  // CrossFeature

}

#endif  // FEATURE_ENGINE_PARSER_CROSS_FEATURE_H_
