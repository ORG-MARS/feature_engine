// File   instant_feature.cpp
// Author lidongming
// Date   2018-10-10 13:24:33
// Brief

#include "feature_engine/parser/instant_feature.h"
#include "feature_engine/parser/parser_manager.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

namespace feature_engine {

InstantFeature::InstantFeature() : cache_feature_map_(nullptr) {
  feature_map_ = std::make_shared<feature_proto::FeaturesMap>();
  // feature_map_ = new feature_proto::FeaturesMap();
}

Status InstantFeature::Parse(bool generate_textual_feature,
    UserProfile& user_profile, const std::shared_ptr<Document>& doc,
    const DocStat& doc_stat, const common_ml_thrift::DocInfo& docinfo) {
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();

  const std::string& business = user_profile.feature_request()->business;
  const std::vector<FeatureConf>& instant_feature_conf_vector
    = feature_conf_parser.instant_doc_feature_conf_vector();
  std::vector<size_t> feature_conf_index;
  for (size_t i = 0; i < instant_feature_conf_vector.size(); i++) {
    const FeatureConf& feature_conf = instant_feature_conf_vector[i];
    if (std::string::npos != feature_conf.business.find(business)) {
      feature_conf_index.push_back(i);
    }
  }

  ParserManager& parser_manager = ParserManager::GetInstance();
  // Manual Parsers
  const auto& manual_parser_map = parser_manager.manual_parser_map();

  FeatureMap* up_feature_map = user_profile.feature_map()->mutable_f();
  FeatureMap* cache_up_feature_map = NULL;
  if (user_profile.cache_feature_map()) {
    cache_up_feature_map = user_profile.cache_feature_map()->mutable_f();
  }

  FeatureMap* doc_feature_map = doc->feature_map()->mutable_f();

  FeatureMap* instant_feature_map = feature_map_->mutable_f();
  FeatureMap* cache_instant_feature_map = NULL;
  if (cache_feature_map_) {
    cache_instant_feature_map = cache_feature_map_->mutable_f();
  }

  Status parse_status;
  std::string feature_method_name;
  for (int index : feature_conf_index) {
    const FeatureConf& feature_conf = instant_feature_conf_vector[index];

    if (cache_instant_feature_map && feature_conf.enable_cache) {
      if (cache_instant_feature_map->find(HASH_FEATURE_ID(feature_conf.id))
          != cache_instant_feature_map->end()) {
        continue;
      }
    }

    feature_method_name = feature_conf.method;
    auto it = manual_parser_map.find(feature_method_name);
    if (unlikely(it == manual_parser_map.end())) {
      LOG(WARNING) << "parser not found name:" << feature_conf.name
                   << " method:" << feature_method_name;
      continue;
    }

    parse_status = it->second(generate_textual_feature, feature_conf, doc_stat,
                              up_feature_map, cache_up_feature_map, doc_feature_map,
                              instant_feature_map, cache_instant_feature_map,
                              &docinfo, instant_feature_map);

#ifndef NDEBUG
    if (!parse_status.ok()) {
      LOG(WARNING) << "parse status:" << parse_status
        << " name:" << feature_conf.name
        << " method:" << feature_method_name;
    } else {
      LOG(INFO) << "parse status:" << parse_status
        << " name:" << feature_conf.name
        << " method:" << feature_method_name;
    }
#endif
  }
  return Status::OK();
}

}  // namespace feature_engine
