#ifndef FEATURE_ENGINE_PARSER_USER_PROFILE_H_
#define FEATURE_ENGINE_PARSER_USER_PROFILE_H_

#include <memory>
#include <string>
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/ml-thrift/gen-cpp/common_ml_types.h"
#include "feature_engine/ml-thrift/gen-cpp/feature_types.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/feature/feature_define.h"

namespace feature_engine {

using commonlib::Status;

class UserProfile {
 public:
  UserProfile();
  Status Parse(bool generate_hash_feature, bool generate_textual_feature);
  void set_common_user_info(const common_ml_thrift::UserInfo* up);

  std::shared_ptr<feature_proto::FeaturesMap>& feature_map() {
    return feature_map_;
  }

  void set_feature_map(std::shared_ptr<feature_proto::FeaturesMap>& feature_map) {
    feature_map_ = feature_map;
  }

  std::shared_ptr<feature_proto::FeaturesMap>& cache_feature_map() {
    return cache_feature_map_;
  }

  void set_cache_feature_map(
      std::shared_ptr<feature_proto::FeaturesMap>& feature_map) {
    cache_feature_map_ = feature_map;
  }

  void set_feature_map(feature_proto::FeaturesMap& feature_map) {
    *feature_map_ = feature_map;
  }

  const common_ml_thrift::UserInfo* user_info() {
    return user_info_;
  }

  const std::vector<std::pair<std::string, double>>& w2v_clustering_rt() {
    return w2v_clustering_rt_;
  }

  const std::vector<std::pair<std::string, double>>& w2v_clustering_hist() {
    return w2v_clustering_hist_;
  }
 
  const std::vector<std::pair<std::string, double>>& manually_interest_rt() {
    return manually_interest_rt_;
  }

  const std::vector<std::pair<std::string, double>>& manually_interest_hist() {
    return manually_interest_hist_;
  }
 
  const std::vector<std::pair<std::string, double>>& manually_interest_neg_rt() {
    return manually_interest_neg_rt_;
  }
 
  const std::vector<std::pair<std::string, double>>& manually_interest_neg_hist() {
    return manually_interest_neg_hist_;
  }
 
  const std::vector<std::pair<std::string, double>>& manually_category_rt() {
    return manually_category_rt_;
  }
 
  const std::vector<std::pair<std::string, double>>& manually_category_hist() {
    return manually_category_hist_;
  }
 
  const std::vector<std::pair<std::string, double>>& video_w2v_clustering_rt() {
    return video_w2v_clustering_rt_;
  }

  const std::vector<std::pair<std::string, double>>& video_w2v_clustering_hist() {
    return video_w2v_clustering_hist_;
  }
 
  const std::vector<std::pair<std::string, double>>& video_manually_interest_rt() {
    return video_manually_interest_rt_;
  }
 
  const std::vector<std::pair<std::string, double>>& video_manually_interest_hist() {
    return video_manually_interest_hist_;
  }
 
  const std::vector<std::pair<std::string, double>>& video_manually_category_rt() {
    return video_manually_category_rt_;
  }
 
  const std::vector<std::pair<std::string, double>>& video_manually_category_hist() {
    return video_manually_category_hist_;
  }

  const std::vector<std::pair<std::string, double>>& video_manually_interest_neg_rt() {
    return video_manually_interest_neg_rt_;
  }

  const std::vector<std::pair<std::string, double>>& video_manually_interest_neg_hist() {
    return video_manually_interest_neg_hist_;
  }

  const std::vector<std::pair<std::string, double>>& video_title_kw_rt() {
    return video_title_kw_rt_;
  }
 
  const std::vector<std::pair<std::string, double>>& video_title_kw_hist() {
    return video_title_kw_hist_;
  }

  const std::vector<std::pair<std::string, double>>& video_manually_kw_rt() {
    return video_manually_kw_rt_;
  }

  const std::vector<std::pair<std::string, double>>& video_manually_kw_hist() {
    return video_manually_kw_hist_;
  }

  void set_feature_request(const feature_thrift::FeatureRequest* r) {
    feature_request_ = r;
  }

  const feature_thrift::FeatureRequest* feature_request() {
    return feature_request_;
  }

  const std::unordered_map<std::string, int>& user_stats() {
    return user_stats_;
  }

  void ParseUserStats();

 private:
  std::shared_ptr<feature_proto::FeaturesMap> feature_map_;
  std::shared_ptr<feature_proto::FeaturesMap> cache_feature_map_;
  std::string user_id_;

  const common_ml_thrift::UserInfo* user_info_;
  const feature_thrift::FeatureRequest* feature_request_;

  std::shared_ptr<std::unordered_map<std::string, std::vector<long int>>> disc_interest_;
  std::unordered_map<std::string, int> user_stats_;

  std::vector<std::pair<std::string, double>> w2v_clustering_rt_;
  std::vector<std::pair<std::string, double>> w2v_clustering_hist_;
  std::vector<std::pair<std::string, double>> manually_interest_rt_;
  std::vector<std::pair<std::string, double>> manually_interest_hist_;
  std::vector<std::pair<std::string, double>> manually_category_rt_;
  std::vector<std::pair<std::string, double>> manually_category_hist_;
  std::vector<std::pair<std::string, double>> manually_interest_neg_rt_;
  std::vector<std::pair<std::string, double>> manually_interest_neg_hist_;
  std::vector<std::pair<std::string, double>> video_w2v_clustering_rt_;
  std::vector<std::pair<std::string, double>> video_w2v_clustering_hist_;
  std::vector<std::pair<std::string, double>> video_manually_interest_rt_;
  std::vector<std::pair<std::string, double>> video_manually_interest_hist_;
  std::vector<std::pair<std::string, double>> video_manually_category_rt_;
  std::vector<std::pair<std::string, double>> video_manually_category_hist_;
  std::vector<std::pair<std::string, double>> video_manually_interest_neg_rt_;
  std::vector<std::pair<std::string, double>> video_manually_interest_neg_hist_;
  std::vector<std::pair<std::string, double>> video_title_kw_rt_;
  std::vector<std::pair<std::string, double>> video_title_kw_hist_;
  std::vector<std::pair<std::string, double>> video_manually_kw_rt_;
  std::vector<std::pair<std::string, double>> video_manually_kw_hist_;
};  // UserProfile

}

#endif  // FEATURE_ENGINE_PARSER_USER_PROFILE_H_
