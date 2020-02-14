// File   cross_feature.cpp
// Author lidongming
// Date   2018-12-25 18:33:54
// Brief

#include "feature_engine/parser/cross_feature.h"
#include "feature_engine/parser/parser_manager.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

namespace feature_engine {

CrossFeature::CrossFeature() {
  feature_map_ = std::make_shared<feature_proto::FeaturesMap>();
}

Status CrossFeature::Parse(bool gen_textual_feature, UserProfile& user_profile,
                           const std::shared_ptr<Document>& doc,
                           InstantFeature& instant_feature,
                           const DocStat& doc_stat) {
  FeatureMap* feature_map = feature_map_->mutable_f();

  FeatureMap* up_feature_map = user_profile.feature_map()->mutable_f();
  FeatureMap* cache_up_feature_map = NULL;
  if (user_profile.cache_feature_map()) {
    cache_up_feature_map = user_profile.cache_feature_map()->mutable_f();
  }

  FeatureMap* doc_feature_map = doc->feature_map()->mutable_f();
  FeatureMap* instant_feature_map = instant_feature.feature_map()->mutable_f();
  FeatureMap* cache_feature_map = NULL;
  if (instant_feature.cache_feature_map()) {
    cache_feature_map = instant_feature.cache_feature_map()->mutable_f();
  }

  // Manual Parsers
  ParserManager& parser_manager = ParserManager::GetInstance();
  const auto& manual_parser_map = parser_manager.manual_parser_map();
  Status parse_status;

  // Get feature_conf by business
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();
  std::vector<size_t> feature_conf_index;
  const std::vector<FeatureConf>& cross_feature_conf_vector
    = feature_conf_parser.cross_feature_conf_vector();
  const std::string& business = user_profile.feature_request()->business;
  for (size_t i = 0; i < cross_feature_conf_vector.size(); i++) {
    const FeatureConf& feature_conf = cross_feature_conf_vector[i];
    if (std::string::npos != feature_conf.business.find(business)) {
      feature_conf_index.push_back(i);
    }
  }

  for (int index : feature_conf_index) {
    const FeatureConf& feature_conf = cross_feature_conf_vector[index];
    const std::string& feature_method_name = feature_conf.method;
    auto it = manual_parser_map.find(feature_method_name);
    if (it == manual_parser_map.end()) {
      LOG(WARNING) << "parser not found name:" << feature_conf.name
                   << " method:" << feature_method_name;
      continue;
    }

    parse_status = it->second(gen_textual_feature, feature_conf, doc_stat,
                   up_feature_map, cache_up_feature_map, doc_feature_map,
                   instant_feature_map, cache_feature_map, NULL, feature_map);
#ifndef NDEBUG
    if (!parse_status.ok()) {
      LOG(WARNING) << "cross feature parse status:" << parse_status
                   << " name:" << feature_conf.name
                   << " method:" << feature_method_name;
    }
#endif
  }
  return Status::OK();
}

}  // namespace feature_engine
