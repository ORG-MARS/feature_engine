// File   feature_conf_parser.cpp
// Author lidongming
// Date   2018-09-07 16:49:33
// Brief

#include "feature_engine/feature/feature_conf_parser.h"
#include "feature_engine/feature/tinyxml2.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/common/common_define.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/commonlib/include/string_utils.h"
#include "feature_engine/deps/commonlib/include/file_utils.h"
// #include <iostream>

namespace feature_engine {

using namespace tinyxml2;
using namespace commonlib;

// Keeping with feature conf
static const std::string kFeatureConfID = "id";
static const std::string kFeatureConfName = "name";
static const std::string kFeatureConfSource = "data_source";
static const std::string kFeatureConfSourceUP = "user_profile";
static const std::string kFeatureConfSourceEP = "explore_profile";
static const std::string kFeatureConfSourceCP = "doc_profile";
static const std::string kFeatureConfSourceOther = "other";
static const std::string kFeatureConfMethod = "method";
static const std::string kFeatureConfParams = "params";
static const std::string kFeatureConfType = "type";
static const std::string kFeatureConfGroup = "group";
static const std::string kFeatureConfDesc = "desc";
static const std::string kFeatureConfOutput = "output";
static const std::string kFeatureConfInstant= "instant";
static const std::string kFeatureConfInput = "input_feat_id";
static const std::string kFeatureConfTrue = "true";
static const std::string kFeatureConfHidden = "hidden";
static const std::string kFeatureConfBusiness= "business";
static const std::string kFeatureConfEnableCache = "enable_cache";

FeatureConfParser::FeatureConfParser() {
  Init();
}

Status FeatureConfParser::Init() {
  return Parse(FLAGS_feature_conf_path);
}

// Parse feature conf from local file in xml format
Status FeatureConfParser::Parse(const std::string& feature_conf_path) {
  parse_status_ = Status::OK();
  std::vector<std::string> feature_conf_files
    = FileUtils::ListDir(feature_conf_path);
  if (feature_conf_files.empty()) {
    parse_status_= Status(error::FEATURE_CONF_PARSE_ERROR,
        "no feature conf file");
  }
  for (const std::string& conf : feature_conf_files) {
    if (!StringUtils::EndsWith(conf, ".xml")) {
      LOG(WARNING) << "ignore feature conf:" << conf;
      continue;
    }
    parse_status_ = ParseFeatureConf(conf);
    if (!parse_status_.ok()) {
      LOG(WARNING) << "parse feature conf error:" << conf;
      break;
    }
  }
  return parse_status_;
}

Status FeatureConfParser::ParseFeatureConf(
    const std::string& feature_conf_file) {
  parse_status_ = Status::OK();
  XMLDocument doc;
  if (doc.LoadFile(feature_conf_file.c_str())) {
    doc.PrintError();
    parse_status_= Status(error::FEATURE_CONF_PARSE_ERROR,
        "Invalid feature conf file");
    return parse_status_;
  }

  // Root element
  XMLElement* root = doc.RootElement();

  // feature_conf element
  XMLElement* feature_conf_element = root->FirstChildElement("feature_conf");
  XMLElement* feature_element
    = feature_conf_element->FirstChildElement("feature");

  // Foreach feature_conf elements
  while (feature_element) {
    FeatureConf feature_conf;
    const XMLAttribute* attr = feature_element->FirstAttribute();
    // Foreach every attribute of every feature_conf element
    while (attr) {
      if (kFeatureConfID == attr->Name()) {                  // feature id
        feature_conf.id = atoi(attr->Value());
      } else if (kFeatureConfName == attr->Name()) {         // feature name
        feature_conf.name = attr->Value();
      } else if (kFeatureConfSource == attr->Name()) {       // feature source
        if (kFeatureConfSourceCP == attr->Value()) {         // doc_profile
          feature_conf.source = DOC_PROFILE;
        } else if (kFeatureConfSourceUP == attr->Value()) {  // user_profiler
          feature_conf.source = USER_PROFILE;
        } else {
          feature_conf.source = OTHER_PROFILE;               // other
        }
      } else if (kFeatureConfMethod == attr->Name()) {       // feature method
        feature_conf.method = attr->Value();
      } else if (kFeatureConfParams == attr->Name()) {       // feature params
        feature_conf.params = attr->Value();
      } else if (kFeatureConfType == attr->Name()) {         // feature type
        // FIXME(lidongming):map value type
        // feature_conf.type = attr->Value();
      } else if (kFeatureConfGroup == attr->Name()) {        // feature group
        feature_conf.group = attr->Value();
      } else if (kFeatureConfDesc == attr->Name()) {         // feature desc
        feature_conf.desc = attr->Value();
      } else if (kFeatureConfOutput == attr->Name()) {       // feature output
        if (kFeatureConfTrue == attr->Value()) {
          feature_conf.output = true;
        } else {
          feature_conf.output = false;
        }
      } else if (kFeatureConfInstant == attr->Name()) {      // instant feature
        if (kFeatureConfTrue == attr->Value()) {
          feature_conf.instant = true;
        } else {
          feature_conf.instant = false;
        }
      } else if ( kFeatureConfInput == attr->Name()) {       // input_feat_id
        std::vector<std::string> depends;
        StringUtils::Split(attr->Value(), ',', depends);
        for (auto& v : depends) {
          feature_conf.depends.push_back(atoi(v.c_str()));
        }
      } else if (kFeatureConfHidden == attr->Name()) {       // hidden
        feature_conf.hidden = (kFeatureConfTrue == attr->Value());
      } else if (kFeatureConfBusiness == attr->Name()) {     // business
        feature_conf.business = attr->Value();
      } else if (kFeatureConfEnableCache == attr->Name()) {  // enable_cache
        if (kFeatureConfTrue == attr->Value()) {
          feature_conf.enable_cache = true;
        } else {
          feature_conf.enable_cache = false;
        }
      }
      attr = attr->Next();
    }

    // FIXME(lidongming):check conf
    if (feature_conf.id <= 0) {
      LOG(WARNING) << "invaid conf";
      feature_element = feature_element->NextSiblingElement("feature");
      continue;
    }

    if (feature_conf_map_.find(feature_conf.id) != feature_conf_map_.end()) {
      LOG(WARNING) << "redudant feature_conf feature_id:" << feature_conf.id
        << " feature_name:" << feature_conf.name;
      feature_element = feature_element->NextSiblingElement("feature");
      continue;
    }

    if (!feature_conf.params.empty()) {
      StringUtils::Split(feature_conf.params, ',', feature_conf.string_params);
      std::string last_v;
      for (const std::string& v : feature_conf.string_params) {
        feature_conf.int_params.push_back(atoi(v.c_str()));
        feature_conf.disc_params.push_back(last_v + "_" + v);
        feature_conf.hash_disc_params.push_back(MAKE_HASH(last_v + "_" + v));
        last_v = v;
      }
      if (feature_conf.string_params.size() != 0) { 
        feature_conf.disc_params.push_back(last_v + "_");
        feature_conf.hash_disc_params.push_back(MAKE_HASH(last_v + "_"));
      }
    }

    feature_conf.name_hash = MAKE_HASH(feature_conf.name);
    feature_conf.id_hash = MAKE_HASH(std::to_string(feature_conf.id));
    feature_conf.feature_name = feature_conf.group + "_" + feature_conf.name;

    // Every Basic feature has its own method implicitly
    if (feature_conf.method == "Input" && feature_conf.output == false) {
      feature_conf.method = feature_conf.feature_name;
      if (feature_conf.source == DOC_PROFILE) {
        if (!feature_conf.instant) {
          doc_feature_conf_vector_.emplace_back(feature_conf);
          feature_conf.conf_type = DOC_FEAUTRE_CONF;
        } else {
          instant_doc_feature_conf_vector_.emplace_back(feature_conf);
          feature_conf.conf_type = INSTANT_FEAUTRE_CONF;
        }
      } else if (feature_conf.source == USER_PROFILE) {
        up_feature_conf_vector_.emplace_back(feature_conf);
        feature_conf.conf_type = UP_FEAUTRE_CONF;
      } else {
        LOG(WARNING) << "feature id:" << feature_conf.id << " unsupported feature source";
        feature_element = feature_element->NextSiblingElement("feature");
        continue;
      }
    } else {
      feature_conf.output = true;
      feature_conf.instant = true;
      if (feature_conf.name.find('+') == std::string::npos) {
        if (feature_conf.source == DOC_PROFILE) {
          instant_doc_feature_conf_vector_.emplace_back(feature_conf);
          feature_conf.conf_type = INSTANT_FEAUTRE_CONF;
        } else if (feature_conf.source == USER_PROFILE) {
          up_feature_conf_vector_.emplace_back(feature_conf);
          feature_conf.conf_type = UP_FEAUTRE_CONF;
        }
      } else {
        std::vector<std::string> feature_names;
        StringUtils::Split(feature_conf.name, '+', feature_names);
        if (feature_names.size() <= 1) {
          LOG(WARNING) << "invaid cross feature conf";
          feature_element = feature_element->NextSiblingElement("feature");
          continue;
        }
        feature_conf.cross = true;
        feature_conf.conf_type = CROSS_FEAUTRE_CONF;
        cross_feature_conf_vector_.emplace_back(feature_conf);
      }
    }

    // feature_conf_map_[feature_conf.method] = feature_conf;
    feature_conf_map_[feature_conf.id] = feature_conf;

    feature_element = feature_element->NextSiblingElement("feature");
  }

  // for (const auto& feature_conf : feature_conf_vector_) {
  //   std::cout << feature_conf.dump() << std::endl;
  // }

  return parse_status_;
}

}  // namespace feature_engine
