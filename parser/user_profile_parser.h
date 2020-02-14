// File   user_profile_parser.h
// Author lidongming
// Date   2018-09-18 16:08:53
// Brief

#ifndef FEATURE_ENGINE_PARSER_USER_PROFILE_PARSER_H_
#define FEATURE_ENGINE_PARSER_USER_PROFILE_PARSER_H_

#include <functional>
#include <map>
#include <set>
#include <vector>
#include "feature_engine/deps/rapidjson/document.h"
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/parser/parser.h"
#include "feature_engine/parser/parser_common.h"  // FeatureMap
#include "feature_engine/parser/user_profile.h"
#include "feature_engine/index/document.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/ml-thrift/gen-cpp/feature_types.h"
#include "feature_engine/deps/commonlib/include/status.h"

namespace feature_engine {

using commonlib::Status;

using UserProfileParserFunc
  = std::function<Status(bool generate_textual_feature, const FeatureConf&,
                         UserProfile*)>;

class UserProfileParser : public Parser {
 public:
  #define DECLARE_USER_PROFILE_PARSER(_group_, _parser_name_) \
   static Status _group_##_##_parser_name_( \
       bool generate_textual_feature, const FeatureConf&, UserProfile*)

  #define DECLARE_AI_USER_PROFILE_PARSER(_parser_name_) \
   DECLARE_USER_PROFILE_PARSER(ai, _parser_name_)
  #define DECLARE_ALGO_USER_PROFILE_PARSER(_parser_name_) \
   DECLARE_USER_PROFILE_PARSER(algo, _parser_name_)

  void parsers(std::vector<std::string>* parsers) const override {
    // FIXME(lidongming):IMPLEMENT
  }
  void ReloadParsers() override {
    // FIXME(lidongming):IMPLEMENT
  }

  void Register() override;

  const std::map<std::string, UserProfileParserFunc>& parser_map() const {
    return parser_map_;
  }

 private:
  void RegisterParser(std::string parser_name, UserProfileParserFunc parser);
  DECLARE_AI_USER_PROFILE_PARSER(devid);
  DECLARE_AI_USER_PROFILE_PARSER(gender);
  DECLARE_AI_USER_PROFILE_PARSER(network);
  DECLARE_AI_USER_PROFILE_PARSER(age);
  DECLARE_AI_USER_PROFILE_PARSER(locationCode);
  DECLARE_AI_USER_PROFILE_PARSER(deviceOS);
  DECLARE_AI_USER_PROFILE_PARSER(deviceBrand);
  DECLARE_AI_USER_PROFILE_PARSER(deviceModel);
  
  DECLARE_AI_USER_PROFILE_PARSER(occupation_info);
  DECLARE_AI_USER_PROFILE_PARSER(orgnization_info);
  DECLARE_AI_USER_PROFILE_PARSER(isNewUser);

  DECLARE_AI_USER_PROFILE_PARSER(timestamp);
  DECLARE_AI_USER_PROFILE_PARSER(clickHistory);

  //-------------------------------ai realtime features------------------------//
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_MANUALLY_INTEREST);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_W2V_CLUSTERING);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_MANUALLY_CATEGORY);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_MANUALLY_INTEREST_NEG);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_VIDEO_MANUALLY_INTEREST_NEG);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_VIDEO_TITLE_KW);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_VIDEO_MANUALLY_CATEGORY);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_VIDEO_W2V_CLUSTERING);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_VIDEO_MANUALLY_KW);
  DECLARE_AI_USER_PROFILE_PARSER(userRtProfile_VIDEO_MANUALLY_INTEREST);

  //---------------------------------ai history features--------------------------------
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_MANUALLY_INTEREST);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_W2V_CLUSTERING);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_MANUALLY_CATEGORY);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_MANUALLY_INTEREST_NEG);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_VIDEO_MANUALLY_INTEREST_NEG);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_VIDEO_TITLE_KW);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_VIDEO_MANUALLY_CATEGORY);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_VIDEO_W2V_CLUSTERING);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_VIDEO_MANUALLY_KW);
  DECLARE_AI_USER_PROFILE_PARSER(userProfile_VIDEO_MANUALLY_INTEREST);

  //----------------------------------algo features---------------------------//
  DECLARE_ALGO_USER_PROFILE_PARSER(uid);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeW2vTopic);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeManuallyInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeManuallyInterestScore);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeManuallyInterestSplit);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeManuallyNegInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeManuallyNegInterestScore);
  DECLARE_ALGO_USER_PROFILE_PARSER(RealtimeManuallyCategory);
  DECLARE_ALGO_USER_PROFILE_PARSER(HistW2vTopic);
  DECLARE_ALGO_USER_PROFILE_PARSER(HistManuallyInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(HistManuallyInterestScore);
  DECLARE_ALGO_USER_PROFILE_PARSER(HistManuallyNegInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(HistManuallyNegInterestScore);
  DECLARE_ALGO_USER_PROFILE_PARSER(HistManuallyCategory);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoRealtimeW2vTopic);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoRealtimeManuallyInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoRealtimeManuallyNegInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoRealtimeManuallyCategory);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoHistW2vTopic);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoHistManuallyInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoHistManuallyNegInterest);
  DECLARE_ALGO_USER_PROFILE_PARSER(VideoHistManuallyCategory);
  DECLARE_ALGO_USER_PROFILE_PARSER(age);
  DECLARE_ALGO_USER_PROFILE_PARSER(gender);
  DECLARE_ALGO_USER_PROFILE_PARSER(NetType);
  DECLARE_ALGO_USER_PROFILE_PARSER(DeviceVendor);
  DECLARE_ALGO_USER_PROFILE_PARSER(DeviceModel);
  DECLARE_ALGO_USER_PROFILE_PARSER(PageFreshNum);
  DECLARE_ALGO_USER_PROFILE_PARSER(PageFreshNumInt);
  DECLARE_ALGO_USER_PROFILE_PARSER(Timestamp);
  DECLARE_ALGO_USER_PROFILE_PARSER(DocTid);
  DECLARE_ALGO_USER_PROFILE_PARSER(ClickHistory);
  DECLARE_ALGO_USER_PROFILE_PARSER(project);
  DECLARE_ALGO_USER_PROFILE_PARSER(locationCode);

  DECLARE_ALGO_USER_PROFILE_PARSER(uid_exp_num);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_click_num);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_user_ctr);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_app_du);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_app_du_median);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_app_du_variance);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_refresh_num);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_refresh_du_interval);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_read_du);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_video_du);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_read_num);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_video_num);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_read_du_median);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_read_du_variance);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_video_du_median);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_video_du_variance );
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_avg_read_du);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_avg_video_du);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_read_progress);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_browse_num);
  DECLARE_ALGO_USER_PROFILE_PARSER(uid_avg_progress);

 private:
  std::map<std::string, UserProfileParserFunc> parser_map_;
};  // UserProfileParser

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_USER_PROFILE_PARSER_H_
