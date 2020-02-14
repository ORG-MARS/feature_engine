// File   document.cpp
// Author lidongming
// Date   2018-09-05 23:05:31
// Brief

#include "feature_engine/index/document.h"
#include <string>
#include <memory>
#include "feature_engine/deps/rapidjson/document.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/parser/parser_manager.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/reloader/data_manager.h"

namespace feature_engine {
using namespace commonlib;

Document::Document()
    : feature_map_(std::make_shared<feature_proto::FeaturesMap>()),
      docid_(""), expire_time_(0) {
}

Status Document::ParseFromJson(const std::string& json_value) {
  ParserManager& parser_manager = ParserManager::GetInstance();
  Status status;

  // Parse json first
  rapidjson::Document json_doc;
  json_doc.Parse(json_value.c_str());
  if (json_doc.HasParseError()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid json value");
  }

  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();
  const std::vector<FeatureConf>& feature_conf_vector
    = feature_conf_parser.doc_feature_conf_vector();
  const std::map<int, FeatureConf>& feature_conf_map
    = feature_conf_parser.feature_conf_map();

  const auto& parser_map = parser_manager.document_parser_map();

  // Foreach feature_conf and invoke feature method registered already
  std::string feature_method_name;
  Status parse_status;
  int total_count = 0;
  int success_count = 0;
  int failed_count = 0;
  auto feature_map = feature_map_->mutable_f();
  for (const auto& feature_conf : feature_conf_vector) {
    feature_method_name = feature_conf.method;
    auto it = parser_map.find(feature_method_name);
    if (it != parser_map.end()) {
      parse_status = it->second(json_doc, feature_conf, feature_map, this);
      if (!parse_status.ok()) {
        failed_count++;
#ifndef NDEBUG
        LOG(WARNING) << "parse status:" << parse_status
                     << " name:" << feature_conf.name
                     << " method:" << feature_method_name
                     << " docid:" << docid_;
#endif
      } else {
        success_count++;
      }
      total_count++;
    } else {
      LOG(WARNING) << "parser not found name:" << feature_conf.name
                   << " method:" << feature_method_name;
    }
  }

  if (!status.ok()) {
    LOG(WARNING) << "parse features error";
    return status;
  }
  return status;
}

void Document::Init() {
  set_docid();
  set_expire_time();
}

Status Document::IsValid() {
  if (FLAGS_load_reserved_docs) {
    reloader::DataManager& data_manager = reloader::DataManager::Instance();
    std::shared_ptr<std::unordered_set<std::string>> reserved_docs
      = data_manager.GetDocDB();
    if (reserved_docs != nullptr) {
      if (reserved_docs->find(docid_) != reserved_docs->end()) {
        // LOG(INFO) << "skip reserved doc:" << docid_;
        return Status::OK();
      }
    }
  }
  int64_t current_time = TimeUtils::GetCurrentTime() / 1000;  // in seconds
  const auto& feature_map = feature_map_->f();
  const auto it = feature_map.find(kFeatureExpireTime);
  if (it == feature_map.end()) {
    return Status(error::DOCUMENT_INVALID, "no expire_time");
  }

  int64_t expire_time = it->second.v_int64();
  if (expire_time > current_time) {
    return Status::OK();
  } else {
    if (FLAGS_debug) {
      LOG(INFO) << "check expire_time:" << expire_time
                << " cur:" << current_time << " docid:" << docid_;
    }
    return Status(error::DOCUMENT_INVALID, "doc expired");
  }
}

void Document::CheckFeatures() {
  LOG(INFO) << "start check document";
  const auto& feature_map = feature_map_->f();

  LOG(INFO) << "feature map size:" << feature_map.size();

  for (const auto& feature_kv : feature_map) {
    LOG(INFO) << "feature_name:" << feature_kv.first;
    const feature_proto::Feature& feature = feature_kv.second;

    switch(feature.kind_case()) {
      case feature_proto::Feature::kVListInt64Double:
        {
          for (int i = 0; i < feature.v_list_int64_double().k_size(); i++) {
            LOG(INFO) << "key:" << feature.v_list_int64_double().k(i);
          }
          for (int i = 0; i < feature.v_list_int64_double().w_size(); i++) {
            LOG(INFO) << "value:" << feature.v_list_int64_double().w(i);
          }
        }
        break;
      case feature_proto::Feature::kVString:
        {
            LOG(INFO) << "value:" << feature.v_string();
        }
        break;
      case feature_proto::Feature::kVInt64:
        {
            LOG(INFO) << "value:" << feature.v_int64();
        }
        break;
      default:
        break;
    }
  }
}

#define STR(str) #str

#define PARSE_INT_DOC_STAT_ITEM(_json_doc_, _name_, _field_) \
do { \
  if (_json_doc_.HasMember(#_field_) && doc[#_field_].IsInt()) { \
    this->stat_items[#_name_] = _json_doc_[#_field_].GetInt() * 100; \
  } \
} while(0)

#define PARSE_DOUBLE_DOC_STAT_ITEM(_json_doc_, _name_, _field_) \
do { \
  if (_json_doc_.HasMember(#_field_) && doc[#_field_].IsDouble()) { \
    this->stat_items[#_name_] = (int)(_json_doc_[#_field_].GetDouble() * 100); \
  } \
} while(0)

int DocStat::Parse(const std::string& json_str) {
  rapidjson::Document doc;
  doc.Parse<0>(json_str.c_str());
  if (doc.HasParseError()) {
    LOG(WARNING) << "parse doc stat error err_code:" << doc.GetParseError()
                 << " json:" << json_str;
    return -1;
  }

  if (!doc.HasMember("docid") || !doc["docid"].IsString()) {
    LOG(WARNING) << "parse doc stat error invalid docid json:" << json_str;
    return -1;
  }

  this->docid = doc["docid"].GetString();

  PARSE_INT_DOC_STAT_ITEM(doc, doc_read_num, read_num);
  PARSE_INT_DOC_STAT_ITEM(doc, doc_post_num, post_num);
  PARSE_INT_DOC_STAT_ITEM(doc, doc_ding_num, ding_num);
  PARSE_INT_DOC_STAT_ITEM(doc, doc_exp_num, exp_num);
  PARSE_INT_DOC_STAT_ITEM(doc, doc_browse_num, browse_num);
  PARSE_INT_DOC_STAT_ITEM(doc, doc_click_num, click_num);
  PARSE_INT_DOC_STAT_ITEM(doc, doc_read_duration, read_duration);
  PARSE_DOUBLE_DOC_STAT_ITEM(doc, doc_read_progress, read_progress);
  PARSE_DOUBLE_DOC_STAT_ITEM(doc, doc_avg_progress, avg_progress);
  PARSE_DOUBLE_DOC_STAT_ITEM(doc, doc_avg_duration, avg_duration);

  return 0;
}

void DocStat::Dump() const {
  // LOG(INFO) << "[DOC_STAT]" << " docid:" << docid
  //           << " read_num:" << read_num
  //           << " post_num:" << post_num
  //           << " ding_num:" << ding_num
  //           << " exp_num:" << exp_num
  //           << " browse_num:" << browse_num
  //           << " click_num:" << click_num
  //           << " read_duration:" << read_duration
  //           << " read_progress:" << read_progress
  //           << " avg_progress:" << avg_progress
  //           << " avg_duration:" << avg_duration
  //           << " timestamp:" << timestamp;
  std::string v = "[DOC_STAT] docid:" + this->docid + " ";
  for (const auto& kv : stat_items) {
    v += " " + kv.first + ":" + std::to_string(kv.second);
  }
  LOG(INFO) << v;
}

}
