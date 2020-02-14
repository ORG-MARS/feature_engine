// File   user_profile_parser.cpp
// Author lidongming
// Date   2018-09-18 23:07:07
// Brief

#include "feature_engine/parser/user_profile_parser.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/common/common_define.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/string_utils.h"

namespace feature_engine {

using namespace commonlib;

static const std::string NEW_USER_STRING("toutiao");

#define USER_PROFILE_PARSER_DEFINITION(_group_, _name_) \
UserProfileParser::_group_##_##_name_( \
    bool generate_textual_feature, \
    const FeatureConf& feature_conf, UserProfile* up)

#define UPDATE_FEATURE_STRING(_value_) \
do { \
  feature_proto::Feature feature; \
  feature.set_v_string(_value_); \
  (*feature_map)[feature_conf.id] = std::move(feature); \
  FeatureLog<std::string>(feature_conf, _value_); \
} while(0)

#define UPDATE_FEATURE_INT32(_value_) \
do { \
  feature_proto::Feature feature; \
  feature.set_v_int32(_value_); \
  (*feature_map)[feature_conf.id] = std::move(feature); \
  FeatureLog<int32_t>(feature_conf, _value_); \
} while(0)

#define UPDATE_FEATURE_INT64(_value_) \
do { \
  feature_proto::Feature feature; \
  feature.set_v_int64(_value_); \
  (*feature_map)[feature_conf.id] = std::move(feature); \
  FeatureLog<int64_t>(feature_conf, _value_); \
} while(0)

#define UPDATE_FEATURE_LISTSTRINGINT64(_value_) \
do { \
  feature_proto::Feature feature; \
  feature.set_allocated_v_list_string_int64(_value_); \
  (*feature_map)[feature_conf.id] = std::move(feature); \
} while(0)
//这句话是while（0）上面的，详细看下那个类型怎么用
//  FeatureLog<int64_t>(feature_conf, _value_);

#define UPDATE_FEATURE_STRING_WITH_HASH(_value_) \
do { \
  feature_proto::Feature feature; \
  feature_proto::Feature hash_feature; \
  feature.set_v_string(_value_); \
  hash_feature.set_v_uint64(MAKE_HASH(_value_)); \
  (*feature_map)[feature_conf.id] = std::move(feature); \
  (*feature_map)[feature_conf.id + kHashFeatureOffset] = std::move(hash_feature); \
  FeatureLog<std::string>(feature_conf, _value_); \
  FeatureLog<uint64_t>(feature_conf, MAKE_HASH(_value_)); \
} while(0)

#define UPDATE_FEATURE_BOOLEAN(_value_) \
do { \
  feature_proto::Feature feature; \
  feature.set_v_bool(_value_); \
  (*feature_map)[feature_conf.id] = std::move(feature); \
  FeatureLog<bool>(feature_conf, _value_); \
} while(0)

// devid
Status USER_PROFILE_PARSER_DEFINITION(ai, devid) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info) {
    UPDATE_FEATURE_STRING(user_info->device_id);
    return Status::OK();
  }
  return Status(error::USER_PROFILE_PARSE_ERROR, "no devid");
}

// age
Status USER_PROFILE_PARSER_DEFINITION(ai, age) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  int age = 0;
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.age) {
    age = user_info->age;
  }
  UPDATE_FEATURE_INT32(age);
  return Status::OK();
}
 
// gender
Status USER_PROFILE_PARSER_DEFINITION(ai, gender) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  std::string gender = "UNKNOW";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.gender) {
    gender = user_info->gender;
  }
  UPDATE_FEATURE_STRING(gender);
  return Status::OK();
}

  
// network
Status USER_PROFILE_PARSER_DEFINITION(ai, network) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  std::string nettype = "null";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.nettype) {
    nettype = user_info->nettype;
  }
  UPDATE_FEATURE_STRING(nettype);
  return Status::OK();
} 
                
// locationCode
Status USER_PROFILE_PARSER_DEFINITION(ai, locationCode) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  std::string location_code = "0";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.location_code) {
    location_code = user_info->location_code;
  }
  UPDATE_FEATURE_STRING(location_code);
  return Status::OK();
}

// occupation_info
Status USER_PROFILE_PARSER_DEFINITION(ai, occupation_info) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.occupation_info) {
    for (const auto& key : user_info->occupation_info) {
      list_string->add_k(key);
    }
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// clickHistory
Status USER_PROFILE_PARSER_DEFINITION(ai, clickHistory) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListStringInt64* list_value = feature.mutable_v_list_string_int64();
 
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.click_doc) {
    for (const auto& data : user_info->click_doc) {
      list_value->add_k(data.element);
      list_value->add_w(data.score);
    }
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
    return Status::OK();
}

Status USER_PROFILE_PARSER_DEFINITION(ai, deviceBrand) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.device_brand) {
    UPDATE_FEATURE_STRING(user_info->device_brand);
    return Status::OK();
  }
  return Status(error::USER_PROFILE_PARSE_ERROR, "no device_brand");
}

Status USER_PROFILE_PARSER_DEFINITION(ai, deviceModel) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.device_model) {
    UPDATE_FEATURE_STRING(user_info->device_model);
    return Status::OK();
  }
  return Status(error::USER_PROFILE_PARSE_ERROR, "no device_model");
}

Status USER_PROFILE_PARSER_DEFINITION(ai, deviceOS) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.device_vendor) {
    UPDATE_FEATURE_STRING(user_info->device_vendor);
    return Status::OK();
  }
  return Status(error::USER_PROFILE_PARSE_ERROR, "no device_vendor");
}

// Feature: isNewUser
// request from 'toutiao' and not wechat and jisu
Status USER_PROFILE_PARSER_DEFINITION(ai, isNewUser) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const feature_thrift::FeatureRequest* feature_request = up->feature_request();
  bool is_new_user = false;
  if (feature_request && feature_request->__isset.project) {
    if (NEW_USER_STRING == feature_request->project) {
      is_new_user = true;
    }
  }
  UPDATE_FEATURE_BOOLEAN(is_new_user);
  return Status::OK();
}

// Timestamp
Status USER_PROFILE_PARSER_DEFINITION(ai, timestamp) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  int64_t timestamp = 0;
  if (user_info && user_info->__isset.timestamp) {
    // 单位为秒
    timestamp = user_info->timestamp;
  }
  UPDATE_FEATURE_INT64(timestamp);
  return Status::OK();
}

Status ParseUpAI(bool generate_textual_feature,
  const std::vector<std::pair<std::string, double>>& up_list,
  const FeatureConf& feature_conf, UserProfile* up) {
  if (up_list.empty()) {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no labels");
  }

  FeatureMap* feature_map = up->feature_map()->mutable_f();
  feature_proto::Feature feature;
  auto* list_string_double = feature.mutable_v_list_string_double();
  for (auto& it : up_list) {
    list_string_double->add_k(it.first);
    list_string_double->add_w(it.second);

#ifndef NDEBUG
    LOG(INFO) << "set user feature:" << feature_conf.id
              << " feature_name:" << feature_conf.name
              << " value:" << it.first;
#endif
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

//-------------------------------rt features----------------------------------//
Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_W2V_CLUSTERING) {
  return ParseUpAI (generate_textual_feature,
      up->w2v_clustering_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_MANUALLY_CATEGORY) {
  return ParseUpAI (generate_textual_feature,
      up->manually_category_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_MANUALLY_INTEREST) {
  return ParseUpAI (generate_textual_feature,
      up->manually_interest_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_MANUALLY_INTEREST_NEG) {
  return ParseUpAI (generate_textual_feature,
      up->manually_interest_neg_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_VIDEO_MANUALLY_INTEREST_NEG) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_interest_neg_rt(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_VIDEO_MANUALLY_CATEGORY) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_category_rt(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_VIDEO_W2V_CLUSTERING) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_w2v_clustering_rt(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_VIDEO_MANUALLY_INTEREST) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_interest_rt(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_VIDEO_TITLE_KW) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_title_kw_rt(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userRtProfile_VIDEO_MANUALLY_KW) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_kw_rt(), feature_conf, up);                             
}

//------------------------------history features------------------------------//
Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_W2V_CLUSTERING) {
  return ParseUpAI (generate_textual_feature,
      up->w2v_clustering_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_MANUALLY_CATEGORY) {
  return ParseUpAI (generate_textual_feature,
      up->manually_category_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_MANUALLY_INTEREST) {
  return ParseUpAI (generate_textual_feature,
      up->manually_interest_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_MANUALLY_INTEREST_NEG) {
  return ParseUpAI (generate_textual_feature,
      up->manually_interest_neg_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_VIDEO_MANUALLY_INTEREST_NEG) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_interest_neg_hist(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_VIDEO_MANUALLY_CATEGORY) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_category_hist(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_VIDEO_W2V_CLUSTERING) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_w2v_clustering_hist(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_VIDEO_MANUALLY_INTEREST) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_interest_hist(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_VIDEO_TITLE_KW) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_title_kw_hist(), feature_conf, up);                             
}

Status USER_PROFILE_PARSER_DEFINITION(ai, userProfile_VIDEO_MANUALLY_KW) {     
  return ParseUpAI (generate_textual_feature,                                        
      up->video_manually_kw_hist(), feature_conf, up);                             
}

//---------------------------------algo features------------------------------//
Status USER_PROFILE_PARSER_DEFINITION(algo, uid) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->uid);
  } else {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no uid");  
  }
  return Status::OK();
}

Status ParseUpAlgo(bool generate_textual_feature,
    const std::vector<std::pair<std::string, double>>& up_list,
    const FeatureConf& feature_conf, UserProfile* up) {
  if (up_list.empty()) {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no labels");
  }

  FeatureMap* feature_map = up->feature_map()->mutable_f();
  feature_proto::Feature feature;
  feature_proto::Feature hash_feature;
  auto* list_string = feature.mutable_v_list_string();
  auto* list_hash = hash_feature.mutable_v_list_uint64();
  for (auto& it : up_list) {
    list_string->add_k(it.first);
    list_hash->add_k(MAKE_HASH(it.first));

#ifndef NDEBUG
    LOG(INFO) << "set user feature:" << feature_conf.id
              << " feature_name:" << feature_conf.name
              << " value:" << it.first;
    LOG(INFO) << "set hash user feature:" << feature_conf.id
              << " feature_name:" << feature_conf.name
              << " value:" << MAKE_HASH(it.first);
#endif
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  (*feature_map)[feature_conf.id + kHashFeatureOffset] = std::move(hash_feature);
  return Status::OK();
}

Status ParseUpWithScoreAlgo(bool generate_textual_feature,
    const std::vector<std::pair<std::string, double>>& up_list,
    const FeatureConf& feature_conf, UserProfile* up) {
  if (up_list.empty()) {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no labels");
  }

  FeatureMap* feature_map = up->feature_map()->mutable_f();
  feature_proto::Feature feature;
  auto* list_string_double = feature.mutable_v_list_string_double();
  for (auto& it : up_list) {
    list_string_double->add_k(it.first);
    list_string_double->add_w(it.second);

#ifndef NDEBUG
    LOG(INFO) << "set user feature:" << feature_conf.id
              << " feature_name:" << feature_conf.name
              << " value:" << it.first
              << " score:" << it.second;
#endif
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeW2vTopic) {  
  return ParseUpAlgo (generate_textual_feature,
      up->w2v_clustering_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeManuallyInterest) {
  return ParseUpAlgo (generate_textual_feature,
      up->manually_interest_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeManuallyInterestScore) {
  return ParseUpWithScoreAlgo (generate_textual_feature,
      up->manually_interest_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeManuallyInterestSplit) {
  if (up->manually_interest_rt().empty()) {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no manually_interest_rt");
  }

  FeatureMap* feature_map = up->feature_map()->mutable_f();
  feature_proto::Feature feature;
  feature_proto::Feature hash_feature;
  auto* list_string = feature.mutable_v_list_string();
  auto* list_hash = hash_feature.mutable_v_list_uint64();
  std::unordered_set<std::string> vset;
  std::vector<std::string> v;
  for (auto& it : up->manually_interest_rt()) {
    vset.insert(it.first);
    if (it.first.find('_') != std::string::npos) {
      v.clear();
      commonlib::StringUtils::Split(it.first, '_', v);
      for (const std::string& sub : v) {
        if (sub.empty()) {
          continue;
        }
        vset.insert(sub);
      }
    }
  }

  for (const std::string& v : vset) {
    list_string->add_k(v);
    list_hash->add_k(MAKE_HASH(v));
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  (*feature_map)[feature_conf.id + kHashFeatureOffset] = std::move(hash_feature);
  return Status::OK();
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeManuallyNegInterest) {
  return ParseUpAlgo(generate_textual_feature,
      up->manually_interest_neg_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeManuallyCategory) {
  return ParseUpWithScoreAlgo (generate_textual_feature,
      up->manually_category_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, HistW2vTopic) {
  return ParseUpAlgo (generate_textual_feature,
      up->w2v_clustering_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, HistManuallyInterest) {
  return ParseUpAlgo (generate_textual_feature,
      up->manually_interest_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, HistManuallyInterestScore) {
  return ParseUpWithScoreAlgo (generate_textual_feature,
      up->manually_interest_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, RealtimeManuallyNegInterestScore) {
  if (up->manually_interest_neg_rt().empty()) {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no manually_interest_neg_rt_score");
  }

  FeatureMap* feature_map = up->feature_map()->mutable_f();
  feature_proto::Feature feature;
  auto* list_string_double = feature.mutable_v_list_string_double();
  for (auto& it : up->manually_interest_neg_rt()) {
    list_string_double->add_k(it.first);
    list_string_double->add_w(it.second);
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

Status USER_PROFILE_PARSER_DEFINITION(algo, HistManuallyNegInterestScore) {
  if (up->manually_interest_neg_hist().empty()) {
    return Status(error::USER_PROFILE_PARSE_ERROR, "no manually_interest_neg_hist_score");
  }

  FeatureMap* feature_map = up->feature_map()->mutable_f();
  feature_proto::Feature feature;
  auto* list_string_double = feature.mutable_v_list_string_double();
  for (auto& it : up->manually_interest_neg_hist()) {
    list_string_double->add_k(it.first);
    list_string_double->add_w(it.second);
  }

  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

Status USER_PROFILE_PARSER_DEFINITION(algo, HistManuallyNegInterest) {
  return ParseUpAlgo (generate_textual_feature,
      up->manually_interest_neg_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, HistManuallyCategory) {
  return ParseUpWithScoreAlgo (generate_textual_feature,
      up->manually_category_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoRealtimeW2vTopic) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_w2v_clustering_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoRealtimeManuallyInterest) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_manually_interest_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoRealtimeManuallyCategory) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_manually_category_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoHistW2vTopic) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_w2v_clustering_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoHistManuallyInterest) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_manually_interest_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoHistManuallyCategory) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_manually_category_hist(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoRealtimeManuallyNegInterest) {
  return ParseUpAlgo(generate_textual_feature,
      up->video_manually_interest_neg_rt(), feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, VideoHistManuallyNegInterest) {
  return ParseUpAlgo (generate_textual_feature,
      up->video_manually_interest_neg_hist(), feature_conf, up);
}

// age
Status USER_PROFILE_PARSER_DEFINITION(algo, age) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  int age = 0;
  if (user_info && user_info->__isset.age) {
    age = user_info->age;
  }
  UPDATE_FEATURE_INT32(age);
  return Status::OK();
}
 
// gender
Status USER_PROFILE_PARSER_DEFINITION(algo, gender) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  static const std::string kGender = "UNKNOW";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.gender) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->gender);
    // LOG(INFO) << "uid:" << user_info->uid << " device_id:"
    //           << user_info->device_id << " gender:" << user_info->gender;
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kGender);
    // LOG(INFO) << "use default gender uid:" << user_info->uid << " device_id:"
    //           << user_info->device_id << " gender:" << kGender;
  }
  return Status::OK();
}

// NetType
Status USER_PROFILE_PARSER_DEFINITION(algo, NetType) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  static const std::string kNettype = "null";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.nettype) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->nettype);
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kNettype);
  }
  return Status::OK();
}

// DeviceVendor
Status USER_PROFILE_PARSER_DEFINITION(algo, DeviceVendor) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  static const std::string kVendor = "null";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.device_vendor) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->device_vendor);
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kVendor);
  }
  return Status::OK();
}

// DeviceModel
Status USER_PROFILE_PARSER_DEFINITION(algo, DeviceModel) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  static const std::string kModel = "null";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.device_model) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->device_model);
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kModel);
  }
  return Status::OK();
}

// PageFreshNum
Status USER_PROFILE_PARSER_DEFINITION(algo, PageFreshNum) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  static const std::string kPageFreshNum = "0";
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.fresh_num) {
    UPDATE_FEATURE_STRING_WITH_HASH(std::to_string(user_info->fresh_num));
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kPageFreshNum);
  }
  return Status::OK();
}

// PageFreshNumInt
Status USER_PROFILE_PARSER_DEFINITION(algo, PageFreshNumInt) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  int32_t kPageFreshNum = 0;
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  if (user_info && user_info->__isset.fresh_num) {
    UPDATE_FEATURE_INT32(user_info->fresh_num);
  } else {
    UPDATE_FEATURE_INT32(kPageFreshNum);
  }
  return Status::OK();
}

// Timestamp
Status USER_PROFILE_PARSER_DEFINITION(algo, Timestamp) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  int64_t timestamp = 0;
  if (user_info && user_info->__isset.timestamp) {
    // 单位为秒
    timestamp = user_info->timestamp;
  }
  UPDATE_FEATURE_INT64(timestamp);
  return Status::OK();
}

Status USER_PROFILE_PARSER_DEFINITION(algo, ClickHistory) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  feature_proto::Feature feature; 
  feature_proto::ListStringInt64* list_value
    = feature.mutable_v_list_string_int64();
  if (user_info->__isset.click_doc) {
    for (const auto& data : user_info->click_doc) {
      list_value->add_k(data.element);
      list_value->add_w(data.score);
    }
  }
  (*feature_map)[feature_conf.id] = std::move(feature); 
  return Status::OK();
}

// DocTid
Status USER_PROFILE_PARSER_DEFINITION(algo, DocTid) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  const static std::string kTidDesc = "null";
  if (user_info && user_info->__isset.tid_desc) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->tid_desc);
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kTidDesc);
  }
  return Status::OK();
}

// project
Status USER_PROFILE_PARSER_DEFINITION(algo, project) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const feature_thrift::FeatureRequest* feature_request = up->feature_request();
  const static std::string kProject = "null";
  if (feature_request && feature_request->__isset.project) {
    UPDATE_FEATURE_STRING_WITH_HASH(feature_request->project);
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kProject);
  }
  return Status::OK();
}

// location_code
Status USER_PROFILE_PARSER_DEFINITION(algo, locationCode) {
  FeatureMap* feature_map = up->feature_map()->mutable_f();
  const common_ml_thrift::UserInfo* user_info = up->user_info();
  const static std::string kLocationCode = "0";
  if (user_info && user_info->__isset.location_code) {
    UPDATE_FEATURE_STRING_WITH_HASH(user_info->location_code);
  } else {
    UPDATE_FEATURE_STRING_WITH_HASH(kLocationCode);
  }
  return Status::OK();
}

Status UserStatFeature(const FeatureConf& feature_conf, UserProfile* up) {
  const std::unordered_map<std::string, int>& user_stats = up->user_stats();
  const auto it = user_stats.find(feature_conf.name);
  if (it != user_stats.end()) {
    FeatureMap* feature_map = up->feature_map()->mutable_f();
    UPDATE_FEATURE_INT32(it->second);
    return Status::OK();
  } else {
#if 0
    LOG(INFO) << "not found:" << feature_conf.name;
    for (const auto& kv : user_stats) {
      LOG(INFO) << "user_stats key:" << kv.first << " value:" << kv.second;
    }
#endif
    return Status(error::USER_PROFILE_PARSE_ERROR,
                  "no user_stats " + feature_conf.name);
  }
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_exp_num) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_click_num) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_user_ctr) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_app_du) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_app_du_median) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_app_du_variance) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_refresh_num) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_refresh_du_interval) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_read_du) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_video_du) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_read_num) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_video_num) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_read_du_median) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_read_du_variance) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_video_du_median) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_video_du_variance ) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_avg_read_du) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_avg_video_du) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_read_progress) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_browse_num) {
  return UserStatFeature(feature_conf, up);
}

Status USER_PROFILE_PARSER_DEFINITION(algo, uid_avg_progress) {
  return UserStatFeature(feature_conf, up);
}

}  // namespace feature_engine
