// File   user_profile_parser_register.cpp
// Author lidongming
// Date   2018-09-19 00:38:27
// Brief


#include "feature_engine/parser/user_profile_parser.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

namespace feature_engine {

using namespace commonlib;

#define REGISTER_PARSER(_parser_) \
  RegisterParser(#_parser_, _parser_);

void UserProfileParser::RegisterParser(std::string parser_name,
                                       UserProfileParserFunc parser) {
  parser_map_[parser_name] = std::move(parser);
}

// Register all feature functions
void UserProfileParser::Register() {
  // Required Features
  REGISTER_PARSER(ai_devid);
  REGISTER_PARSER(ai_age);
  REGISTER_PARSER(ai_gender);
  REGISTER_PARSER(ai_network);
  REGISTER_PARSER(ai_locationCode);
  REGISTER_PARSER(ai_occupation_info);
  REGISTER_PARSER(ai_deviceBrand);
  REGISTER_PARSER(ai_deviceModel);
  REGISTER_PARSER(ai_deviceOS);
  REGISTER_PARSER(ai_isNewUser);
  REGISTER_PARSER(ai_clickHistory);
  REGISTER_PARSER(ai_timestamp);

  //--------------------------------ai features-------------------------------//
  // RT Features
  REGISTER_PARSER(ai_userRtProfile_MANUALLY_INTEREST);          
  REGISTER_PARSER(ai_userRtProfile_W2V_CLUSTERING);
  REGISTER_PARSER(ai_userRtProfile_MANUALLY_CATEGORY);
  REGISTER_PARSER(ai_userRtProfile_MANUALLY_INTEREST_NEG);
  REGISTER_PARSER(ai_userRtProfile_VIDEO_MANUALLY_INTEREST_NEG);
  REGISTER_PARSER(ai_userRtProfile_VIDEO_MANUALLY_CATEGORY);
  REGISTER_PARSER(ai_userRtProfile_VIDEO_W2V_CLUSTERING);
  REGISTER_PARSER(ai_userRtProfile_VIDEO_MANUALLY_INTEREST);
  REGISTER_PARSER(ai_userRtProfile_VIDEO_MANUALLY_KW);
  REGISTER_PARSER(ai_userRtProfile_VIDEO_TITLE_KW);

  // Hist Features
  REGISTER_PARSER(ai_userProfile_MANUALLY_INTEREST);
  REGISTER_PARSER(ai_userProfile_W2V_CLUSTERING);
  REGISTER_PARSER(ai_userProfile_MANUALLY_CATEGORY);
  REGISTER_PARSER(ai_userProfile_MANUALLY_INTEREST_NEG);
  REGISTER_PARSER(ai_userProfile_VIDEO_MANUALLY_INTEREST_NEG);
  REGISTER_PARSER(ai_userProfile_VIDEO_MANUALLY_CATEGORY);
  REGISTER_PARSER(ai_userProfile_VIDEO_W2V_CLUSTERING);
  REGISTER_PARSER(ai_userProfile_VIDEO_MANUALLY_INTEREST);
  REGISTER_PARSER(ai_userProfile_VIDEO_MANUALLY_KW);
  REGISTER_PARSER(ai_userProfile_VIDEO_TITLE_KW);

  //--------------------------------algo features-----------------------------//
  REGISTER_PARSER(algo_uid);
  REGISTER_PARSER(algo_RealtimeW2vTopic);
  REGISTER_PARSER(algo_RealtimeW2vTopic);
  REGISTER_PARSER(algo_RealtimeManuallyInterest);
  REGISTER_PARSER(algo_RealtimeManuallyInterestScore);
  REGISTER_PARSER(algo_RealtimeManuallyInterestSplit);
  REGISTER_PARSER(algo_RealtimeManuallyNegInterest);
  REGISTER_PARSER(algo_RealtimeManuallyNegInterestScore);
  REGISTER_PARSER(algo_RealtimeManuallyCategory);
  REGISTER_PARSER(algo_HistW2vTopic);
  REGISTER_PARSER(algo_HistManuallyInterest);
  REGISTER_PARSER(algo_HistManuallyInterestScore);
  REGISTER_PARSER(algo_HistManuallyNegInterest);
  REGISTER_PARSER(algo_HistManuallyNegInterestScore);
  REGISTER_PARSER(algo_HistManuallyCategory);
  REGISTER_PARSER(algo_VideoRealtimeW2vTopic);
  REGISTER_PARSER(algo_VideoRealtimeManuallyInterest);
  REGISTER_PARSER(algo_VideoRealtimeManuallyNegInterest);
  REGISTER_PARSER(algo_VideoRealtimeManuallyCategory);
  REGISTER_PARSER(algo_VideoHistW2vTopic);
  REGISTER_PARSER(algo_VideoHistManuallyInterest);
  REGISTER_PARSER(algo_VideoHistManuallyNegInterest);
  REGISTER_PARSER(algo_VideoHistManuallyCategory);
  REGISTER_PARSER(algo_age);
  REGISTER_PARSER(algo_gender);
  REGISTER_PARSER(algo_NetType);
  REGISTER_PARSER(algo_DeviceVendor);
  REGISTER_PARSER(algo_DeviceModel);
  REGISTER_PARSER(algo_PageFreshNum);
  REGISTER_PARSER(algo_PageFreshNumInt);
  REGISTER_PARSER(algo_Timestamp);
  REGISTER_PARSER(algo_DocTid);
  REGISTER_PARSER(algo_ClickHistory);
  REGISTER_PARSER(algo_project);
  REGISTER_PARSER(algo_locationCode);

  REGISTER_PARSER(algo_uid_exp_num);
  REGISTER_PARSER(algo_uid_click_num);
  REGISTER_PARSER(algo_uid_user_ctr);
  REGISTER_PARSER(algo_uid_app_du);
  REGISTER_PARSER(algo_uid_app_du_median);
  REGISTER_PARSER(algo_uid_app_du_variance);
  REGISTER_PARSER(algo_uid_refresh_num);
  REGISTER_PARSER(algo_uid_refresh_du_interval);
  REGISTER_PARSER(algo_uid_read_du);
  REGISTER_PARSER(algo_uid_video_du);
  REGISTER_PARSER(algo_uid_read_num);
  REGISTER_PARSER(algo_uid_video_num);
  REGISTER_PARSER(algo_uid_read_du_median);
  REGISTER_PARSER(algo_uid_read_du_variance);
  REGISTER_PARSER(algo_uid_video_du_median);
  REGISTER_PARSER(algo_uid_video_du_variance );
  REGISTER_PARSER(algo_uid_avg_read_du);
  REGISTER_PARSER(algo_uid_avg_video_du);
  REGISTER_PARSER(algo_uid_read_progress);
  REGISTER_PARSER(algo_uid_browse_num);
  REGISTER_PARSER(algo_uid_avg_progress);

}

}  // namespace feature_engine
