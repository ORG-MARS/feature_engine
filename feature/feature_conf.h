// File   feature.h
// Author lidongming
// Date   2018-08-30 16:30:37
// Brief

#ifndef FEATURE_ENGINEERING_FEATURE_FEATURE_CONF_H_
#define FEATURE_ENGINEERING_FEATURE_FEATURE_CONF_H_

#include <sstream>
#include <string>

namespace feature_engine {

enum FeatureSource {
  DOC_PROFILE = 0,
  USER_PROFILE,
  OTHER_PROFILE,
};

enum FeatureValueType {
  FEATURE_VALUE_STRING = 0,
  FEATURE_VALUE_UINT32,
  FEATURE_VALUE_SEGMENT,
};

enum FeatureConfType {
  DOC_FEAUTRE_CONF = 0,
  UP_FEAUTRE_CONF,
  INSTANT_FEAUTRE_CONF,
  CROSS_FEAUTRE_CONF,
  NO_FEAUTRE_CONF_TYPE
};

struct FeatureConf {
  int id;
  std::string group;
  std::string name;
  uint64_t name_hash = 0;
  uint64_t id_hash = 0;
  std::string feature_name;  // group_name, eg:ai_age, algo_gender
  std::string alias_name;
  FeatureSource source = OTHER_PROFILE;
  std::string method;
  std::string params;
  std::vector<int> int_params;
  std::vector<std::string> string_params;
  std::vector<std::string> disc_params;  // eg:_0, 0_1, 1_2, 2_3, 3_
  std::vector<uint64_t> hash_disc_params;  // hash of every disc param
  FeatureValueType value_type;
  std::string desc;
  bool output = false;
  std::vector<int> depends;
  bool hidden = false;
  bool instant = false;
  bool cross = false;
  FeatureConfType conf_type;
  std::string business;
  bool enable_cache = false;

  std::string dump() const {
    std::stringstream ss;
    ss << "id:" << id << " name:" << name
       << " feature_name:" << feature_name
       << " method:" << method << " params:" << params
       << " desc:" << desc;
    return ss.str();
  }
};

}  // namespace feature_engine

#endif
