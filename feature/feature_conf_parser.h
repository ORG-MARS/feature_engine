// File   feature_conf_parser.h
// Author lidongming
// Date   2018-09-07 16:49:39
// Brief

#ifndef FEATURE_ENGINE_FEATURE_FEATURE_CONF_PARSER_H_
#define FEATURE_ENGINE_FEATURE_FEATURE_CONF_PARSER_H_

#include <map>
#include <vector>
#include <string>
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/deps/commonlib/include/status.h"

namespace feature_engine {

using commonlib::Status;

// Singleton
class FeatureConfParser {
 public:
   static FeatureConfParser& GetInstance() {
     static FeatureConfParser instance;
     return instance;
   }

   FeatureConfParser();
   // ~FeatureConfParser();

   Status Init();

   std::vector<FeatureConf>& doc_feature_conf_vector() {
     return doc_feature_conf_vector_;
   }

   std::vector<FeatureConf>& up_feature_conf_vector() {
     return up_feature_conf_vector_;
   }

   std::vector<FeatureConf>& explore_feature_conf_vector() {
     return explore_feature_conf_vector_;
   }

   std::vector<FeatureConf>& instant_doc_feature_conf_vector() {
     return instant_doc_feature_conf_vector_;
   }

   // std::vector<FeatureConf>& single_up_feature_conf_vector() {
     // return single_up_feature_conf_vector_;
   // }

   std::vector<FeatureConf>& cross_feature_conf_vector() {
     return cross_feature_conf_vector_;
   }

   // std::map<std::string, FeatureConf>& feature_conf_map() {
   std::map<int, FeatureConf>& feature_conf_map() {
     return feature_conf_map_;
   }

   Status parse_status() {
     return parse_status_;
   }

   FeatureConf* GetFeatureConf(int feature_id) {
     auto it = feature_conf_map_.find(feature_id);
     if (it != feature_conf_map_.end()) {
       return &it->second;
     }
     return NULL;
   }


   FeatureSource GetFeatureSource(int feature_id) {
     auto it = feature_conf_map_.find(feature_id);
     if (it != feature_conf_map_.end()) {
       return it->second.source;
     }
     return OTHER_PROFILE;
   }

   uint64_t GetFeatureNameHash(int feature_id) {
     auto it = feature_conf_map_.find(feature_id);
     if (it != feature_conf_map_.end()) {
       return it->second.name_hash;
     }
     return 0;
   }

   std::string GetFeatureName(int feature_id) {
     auto it = feature_conf_map_.find(feature_id);
     if (it != feature_conf_map_.end()) {
       return it->second.name;
     }
     return "";
   }

 private:
   Status Parse(const std::string& feature_conf_path);
   Status ParseFeatureConf(const std::string& feature_conf_file);
   Status parse_status_;  // used for unittest

   std::vector<FeatureConf> doc_feature_conf_vector_;
   std::vector<FeatureConf> up_feature_conf_vector_;
   std::vector<FeatureConf> explore_feature_conf_vector_;
   std::vector<FeatureConf> instant_doc_feature_conf_vector_;
   // std::vector<FeatureConf> single_up_feature_conf_vector_;
   std::vector<FeatureConf> cross_feature_conf_vector_;
   // std::map<std::string, FeatureConf> feature_conf_map_;
   std::map<int, FeatureConf> feature_conf_map_;
};  // FeatureConfParser
}  // namespace feature_engine

#endif  // FEATURE_ENGINE_FEATURE_FEATURE_CONF_PARSER_H_
