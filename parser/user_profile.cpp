// File   user_profile.cpp
// Author lidongming
// Date   2018-09-18 23:05:49
// Brief

#include "feature_engine/parser/user_profile.h"
#include "feature_engine/parser/parser_manager.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/reloader/data_manager.h"

#include <sstream>

namespace feature_engine {

UserProfile::UserProfile() : cache_feature_map_(nullptr), user_info_(NULL) {
  feature_map_ = std::make_shared<feature_proto::FeaturesMap>();
}

Status UserProfile::Parse(bool generate_hash_feature, bool generate_textual_feature) {
  ParseUserStats();

  const std::string& business = feature_request_->business;
  // Get feature conf
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();
  const std::vector<FeatureConf>& up_feature_conf_vector =
    feature_conf_parser.up_feature_conf_vector();
  std::vector<size_t> feature_conf_index;
  for (size_t i = 0; i < up_feature_conf_vector.size(); i++) {
    const FeatureConf& feature_conf = up_feature_conf_vector[i];
    if (std::string::npos != feature_conf.business.find(business)) {
      feature_conf_index.push_back(i);
    }
  }

  ParserManager& parser_manager = ParserManager::GetInstance();
  // UP Parsers
  const auto& up_parser_map = parser_manager.up_parser_map();
  // Manual Parsers
  const auto& manual_parser_map = parser_manager.manual_parser_map();

  FeatureMap* feature_map = feature_map_->mutable_f();
  FeatureMap* cache_feature_map = NULL;
  if (cache_feature_map_) {
    cache_feature_map = cache_feature_map_->mutable_f();
  }
  std::string feature_method_name;
  DocStat doc_stat;  // NOT USED, placeholder for manual parsers
  Status parse_status;

  for (size_t index : feature_conf_index) {
    const FeatureConf& feature_conf = up_feature_conf_vector[index];

    if (cache_feature_map && feature_conf.enable_cache) {
      if (cache_feature_map->find(HASH_FEATURE_ID(feature_conf.id))
          != feature_map->end()) {
        continue;
      }
    }

    feature_method_name = feature_conf.method;
    if (!feature_conf.output) {
      auto it = up_parser_map.find(feature_method_name);
      if (it != up_parser_map.end()) {
        it->second(generate_textual_feature, feature_conf, this);
      }
    } else {
      auto it = manual_parser_map.find(feature_method_name);
      if (it != manual_parser_map.end()) {
        parse_status = it->second(generate_textual_feature, feature_conf, doc_stat,
                                  feature_map, cache_feature_map,
                                  NULL, NULL, NULL, NULL,
                                  feature_map);
#ifndef NDEBUG
      LOG(INFO) << "parse status:" << parse_status
                << " name:" << feature_conf.name
                << " method:" << feature_method_name;
#endif
      }
    }
  }

  // Cache up
  // FeatureCacheItem new_cached_up;
  // new_cached_up.update_time = TimeUtils::GetCurrentTime();
  // new_cached_up.feature_map = user_profile->feature_map();
  // up_cache.Put(uid, new_cached_up);

  return Status::OK();
}

void UserProfile::set_common_user_info(const common_ml_thrift::UserInfo* user_info) {
  if (user_info == NULL) {
    return;
  }
  user_info_ = user_info;

  if (user_info->__isset.user_realtime_profile) {
    for (const auto& up : user_info->user_realtime_profile) {
      if (up.__isset.type && up.__isset.key) {
          switch (up.type) {
          case common_ml_thrift::LABEL_TYPE::W2V_CLUSTERING:
            w2v_clustering_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::MANUALLY_INTEREST:
            manually_interest_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::MANUALLY_INTEREST_NEG:
            manually_interest_neg_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::MANUALLY_CATEGORY:
            manually_category_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_W2V_CLUSTERING:
            video_w2v_clustering_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_INTEREST:
            video_manually_interest_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_CATEGORY:
            video_manually_category_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_INTEREST_NEG:
            video_manually_interest_neg_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_KW:
            video_manually_kw_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_TITLE_KW:
            video_title_kw_rt_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          default:
            break;
        }
      }
    }
  }

  if (user_info->__isset.user_longterm_profile) {
    for (const auto& up : user_info->user_longterm_profile) {
      if (up.__isset.type && up.__isset.key) {
        switch (up.type) {
          case common_ml_thrift::LABEL_TYPE::W2V_CLUSTERING:
            w2v_clustering_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::MANUALLY_INTEREST:
            manually_interest_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::MANUALLY_INTEREST_NEG:
            manually_interest_neg_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::MANUALLY_CATEGORY:
            manually_category_hist_.emplace_back(
                std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_W2V_CLUSTERING:
            video_w2v_clustering_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_INTEREST:
            video_manually_interest_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_CATEGORY:
            video_manually_category_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_INTEREST_NEG:
            video_manually_interest_neg_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_MANUALLY_KW:
            video_manually_kw_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          case common_ml_thrift::LABEL_TYPE::VIDEO_TITLE_KW:
            video_title_kw_hist_.emplace_back(std::pair<std::string, double>(up.key, up.score));
            break;
          default:
            break;
        }
      }
    }
  } 
}

void UserProfile::ParseUserStats() {
  if (user_info_ == NULL || !user_info_->__isset.user_stats) {
    LOG(WARNING) << "no user stats";
    return;
  }

  for (const auto& kv : user_info_->user_stats) {
    user_stats_["uid_" + kv.first] = atof(kv.second.c_str()) * 100;
  }

#if 0
  for (const auto& kv : user_stats_) {
    LOG(INFO) << " USER_STAT key:" << kv.first << " value:" << kv.second;
  }
#endif
}

}  // namespace feature_engine
