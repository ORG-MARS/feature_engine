// File   document_profile_parser.cpp
// Author lidongming
// Date   2018-09-06 09:15:10
// Brief

// Feature parser lib
// Main parser lib entry for features

#include "feature_engine/parser/document_profile_parser.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/string_utils.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/common/common_define.h"
#include "feature_engine/common/common_gflags.h"
#include <boost/algorithm/string.hpp>

namespace feature_engine {

using namespace commonlib;

#define DOCUMENT_PROFILE_PARSER_DEFINITION(_group_, _name_) \
  DocumentProfileParser::_group_##_##_name_( \
    const rapidjson::Document& json_doc, \
    const FeatureConf& feature_conf, \
    FeatureMap* feature_map, \
    Document* document)

#define BASE_DOCUMENT_PROFILE_PARSER_DEFINITION(_name_) \
  DOCUMENT_PROFILE_PARSER_DEFINITION(base, _name_)

#define AI_DOCUMENT_PROFILE_PARSER_DEFINITION(_name_) \
  DOCUMENT_PROFILE_PARSER_DEFINITION(ai, _name_)

#define ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(_name_) \
  DOCUMENT_PROFILE_PARSER_DEFINITION(algo, _name_)

#define CONCAT(a, b) (#a" "#b)
#define CHECK_JSON_ITEM_EXISTS_OR_RETURN(_json_doc_, _field_) \
  if (!_json_doc_.HasMember(#_field_)) { \
    return Status(error::DOCUMENT_PARSE_ERROR, CONCAT(not found, _field_)); \
  }

template<typename T>
void FeatureLog(const FeatureConf& feature_conf, Document* document,
    const T& value) {
#ifndef NDEBUG
  LOG(INFO) << "set feature:" << feature_conf.id
            << " feature_name:" << feature_conf.name
            << " docid:" << document->docid() << " value:" << value;
#endif
}

#define UPDATE_STRING_FEATURE(_json_value_) \
do { \
  const std::string& value = _json_value_.GetString(); \
  if (!value.empty()) { \
    feature_proto::Feature feature; \
    feature.set_v_string(value); \
    (*feature_map)[feature_conf.id] = std::move(feature); \
    FeatureLog<std::string>(feature_conf, document, value); \
  } \
} while(0)

#define UPDATE_STRING_FEATURE_HASH(_json_value_) \
do { \
  const std::string& value = _json_value_.GetString(); \
  if (!value.empty()) { \
    feature_proto::Feature feature; \
    feature.set_v_uint64(MAKE_HASH(value)); \
    (*feature_map)[feature_conf.id + kHashFeatureOffset] = std::move(feature); \
    FeatureLog<uint64_t>(feature_conf, document, MAKE_HASH(value)); \
  } \
} while(0)

//---------------------------------ai features--------------------------------//
// Required Feature:docid
// Feature:docid
Status DOCUMENT_PROFILE_PARSER_DEFINITION(base, docid) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, docid);
  const rapidjson::Value& docid_value = json_doc["docid"];
  if (!docid_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid docid type");
  }
  std::string docid = docid_value.GetString();
  if (docid.empty()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid docid value");
  }
  feature_proto::Feature feature;
  feature.set_v_string(docid);
  (*feature_map)[feature_conf.id] = std::move(feature);
  document->set_docid(docid);
  return Status::OK();
}

// Feature:docid_hash
Status DOCUMENT_PROFILE_PARSER_DEFINITION(base, docid_hash) {
  feature_proto::Feature feature;
  feature.set_v_uint64(MAKE_HASH(document->docid()));
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:expire_time
// 这个字段之前是string类型，pctr的redis将要废弃，以后所有都改为int64
Status DOCUMENT_PROFILE_PARSER_DEFINITION(base, expire_time) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, expireTime);
  if (!json_doc.HasMember("expireTime")) {
    return Status(error::DOCUMENT_PARSE_ERROR, "no expireTime");
  }

  const rapidjson::Value& expire_time_value = json_doc["expireTime"];
  if (!expire_time_value.IsInt64()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid expireTime type");
  }
  int64_t expire_time = expire_time_value.GetInt64();

  int64_t expire_time2 = 0;
  if (json_doc.HasMember("expireTime2")) {
    const rapidjson::Value& expire_time2_value = json_doc["expireTime2"];
    if (expire_time2_value.IsInt64()) {
      expire_time2 = expire_time2_value.GetInt64();
    }
  }

  if (expire_time < expire_time2) {
    expire_time = expire_time2;
  }

  feature_proto::Feature feature;
  feature.set_v_int64(expire_time);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:titleSegList
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(titleSegList) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, titleSegList);
  const rapidjson::Value& json_array = json_doc["titleSegList"];
  if (!json_array.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid titleSegList type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  for (int i = 0; i < json_array.Size(); i++) {
    const rapidjson::Value& json_value = json_array[i];
    if (json_value.IsString()) {
      list_string->add_k(json_value.GetString());
    }
  }
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:source_type
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(source_type) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, source_type);
  const rapidjson::Value& json_value = json_doc["source_type"];
  // FIXME(lidongming):check type of 'source_type', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid source_type type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:quality
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(quality) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, quality);
  const rapidjson::Value& json_value = json_doc["quality"];
  if (!json_value.IsDouble()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid quality type");
  }
  double value = json_value.GetDouble();
  feature_proto::Feature feature;
  feature.set_v_double(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:site_id
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(site_id) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, site_id);
  const rapidjson::Value& json_value = json_doc["site_id"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid site_id type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:accountOriginal
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(accountOriginal) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, accountOriginal);
  const rapidjson::Value& json_value = json_doc["accountOriginal"];
  if (!json_value.IsBool()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid accountOriginal type");
  }
  bool value = json_value.GetBool();
  feature_proto::Feature feature;
  feature.set_v_bool(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

//-------------------------------AI Features----------------------------------//
// Feature:doc_byte_count
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(doc_byte_count) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, doc_byte_count);
  const rapidjson::Value& json_value = json_doc["doc_byte_count"];
  // FIXME(lidongming):check type of 'doc_byte_count', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid doc_byte_count type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:localcity
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(localcity) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, localcity);
  const rapidjson::Value& json_value = json_doc["localcity"];
  if (!json_value.IsBool()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid localcity type");
  }
  bool value = json_value.GetBool();
  feature_proto::Feature feature;
  feature.set_v_bool(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:is_santu
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(is_santu) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, is_santu);
  const rapidjson::Value& json_value = json_doc["is_santu"];
  // FIXME(lidongming):check type of 'is_santu', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid is_santu type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:doctype
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(doctype) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, doctype);
  const rapidjson::Value& json_value = json_doc["doctype"];
  // FIXME(lidongming):check type of 'doctype', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid doctype type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:origion
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(origion) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, origion);
  const rapidjson::Value& json_value = json_doc["origion"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid origion type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:title_word_count
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(title_word_count) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, title_word_count);
  const rapidjson::Value& json_value = json_doc["title_word_count"];
  // FIXME(lidongming):check type of 'title_word_count', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid title_word_count type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:interests
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(interests) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, interests);
  const rapidjson::Value& json_value = json_doc["interests"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid interests type");
  }

  std::vector<std::string> value_array;
  std::string value = json_value.GetString();
  commonlib::StringUtils::Split(value, ",", value_array);

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  for (std::string& v : value_array) {
    list_string->add_k(std::move(v));
  }
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:source
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(source) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, source);
  const rapidjson::Value& json_value = json_doc["source"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid source type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:title_char_count
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(title_char_count) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, title_char_count);
  const rapidjson::Value& json_value = json_doc["title_char_count"];
  // FIXME(lidongming):check type of 'title_char_count', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid title_char_count type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:doc_char_count
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(doc_char_count) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, doc_char_count);
  const rapidjson::Value& json_value = json_doc["doc_char_count"];
  // FIXME(lidongming):check type of 'doc_char_count', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid doc_char_count type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:sansu_score
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(sansu_score) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, sansu_score);
  const rapidjson::Value& json_value = json_doc["sansu_score"];
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid sansu_score type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:topic
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(topic) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, topic);
  const rapidjson::Value& json_value = json_doc["topic"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid topic type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:accountCategory
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(accountCategory) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, accountCategory);
  const rapidjson::Value& json_value = json_doc["accountCategory"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid accountCategory type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:sourcelevel
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(sourceLevel) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, sourceLevel);
  const rapidjson::Value& json_value = json_doc["sourceLevel"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid sourceLevel type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:vulgar
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(vulgar) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, vulgar);
  const rapidjson::Value& json_value = json_doc["vulgar"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid vulgar type");
  }
  std::string value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_double(atof(value.c_str()));
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:dkeys
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(dkeys) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, dkeys);
  const rapidjson::Value& json_value = json_doc["dkeys"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid dkeys type");
  }

  std::vector<std::string> value_array;
  std::string value = json_value.GetString();
  StringUtils::Split(value, ",", value_array);

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  for (std::string& v : value_array) {
    list_string->add_k(std::move(v));
  }
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:qualityLevel
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(qualityLevel) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, qualityLevel);
  const rapidjson::Value& json_value = json_doc["qualityLevel"];
  // FIXME(lidongming):check type of 'qualityLevel', int or uint?
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid qualityLevel type");
  }
  int value = json_value.GetInt();
  feature_proto::Feature feature;
  feature.set_v_int32(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:category
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(category) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, category);
  const rapidjson::Value& json_value = json_doc["category"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid category type");
  }

  std::vector<std::string> value_array;
  std::string value = json_value.GetString();
  StringUtils::Split(value, ",", value_array);

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  for (std::string& v : value_array) {
    list_string->add_k(std::move(v));
  }
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:topic_w2v
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(topic_w2v) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, w2v_topic);
//  新的mysql字段更新
  const rapidjson::Value& topic_w2v = json_doc["w2v_topic"];
  // topic_w2v is an array
  if (!topic_w2v.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid w2v type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListInt64Double* v = feature.mutable_v_list_int64_double();

  char * pEnd = NULL;
  for (int i = 0; i < topic_w2v.Size(); i++) {
    const rapidjson::Value& sub_topic_w2v = topic_w2v[i];

    // Process special lables:W2V_CLUSTERING
    if (sub_topic_w2v.HasMember("topic_id")
        && sub_topic_w2v.HasMember("score")) {

      // Store label_id and label_score in PairList
      std::string label_id = sub_topic_w2v["topic_id"].GetString();
      float label_score = sub_topic_w2v["score"].GetDouble();
      v->add_k(strtol(label_id.c_str(), &pEnd, 10));
      v->add_w(label_score);
    }
  }
  return Status::OK();
}

// Feature:data_source
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(data_source) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, data_source);
  const rapidjson::Value& json_value = json_doc["data_source"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid data_source type");
  }
  const std::string& value = json_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  return Status::OK();
}

// Feature:title_kw
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(title_kw) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, title_kw);
  const rapidjson::Value& title_kw = json_doc["title_kw"];
  // title_kw is an array
  if (!title_kw.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid title_kw type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListStringDouble* v = feature.mutable_v_list_string_double();

  for (int i = 0; i < title_kw.Size(); i++) {
    const rapidjson::Value& sub_title_kw = title_kw[i];

    // Process special lables:TITLE_KW
    if (sub_title_kw.HasMember("word")
        && sub_title_kw.HasMember("score")) {

      // Store label_word and label_score in PairList
      std::string label_word = sub_title_kw["word"].GetString();
      float label_score = sub_title_kw["score"].GetDouble();
      v->add_k(label_word);
      v->add_w(label_score);
    }
  }
  return Status::OK();
}

// Feature:title_kw_v2
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(title_kw_v2) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, title_kw_v2);
  const rapidjson::Value& title_kw = json_doc["title_kw_v2"];
  // title_kw is an array
  if (!title_kw.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid title_kw_v2 type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListStringDouble* v = feature.mutable_v_list_string_double();

  for (int i = 0; i < title_kw.Size(); i++) {
    const rapidjson::Value& sub_title_kw = title_kw[i];

    // Process special lables:TITLE_KW
    if (sub_title_kw.HasMember("label_type")
        && sub_title_kw["label_type"] == "TITLE_KW_V2"
        && sub_title_kw.HasMember("label_word")
        && sub_title_kw.HasMember("label_score")) {

      // Store label_word and label_score in PairList
      std::string label_word = sub_title_kw["label_word"].GetString();
      float label_score = sub_title_kw["label_score"].GetDouble();
      v->add_k(label_word);
      v->add_w(label_score);
    }
  }
  return Status::OK();
}

// Feature:articleEntity
Status AI_DOCUMENT_PROFILE_PARSER_DEFINITION(articleEntity) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, articleEntity);
  const rapidjson::Value& article_entity = json_doc["articleEntity"];
  // article_entity is an array
  if (!article_entity.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid articleEntity type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListStringDouble* v = feature.mutable_v_list_string_double();

  for (int i = 0; i < article_entity.Size(); i++) {
    const rapidjson::Value& sub_article_entity = article_entity[i];

    if (sub_article_entity.HasMember("tag")
        && sub_article_entity.HasMember("score")) {

      // Store tag and score in PairList
      std::string tag = sub_article_entity["tag"].GetString();
      float score = sub_article_entity["score"].GetDouble();
      v->add_k(tag);
      v->add_w(score);
    }
  }
  return Status::OK();
}

//-------------------------------algo features--------------------------------//
// Feature:DocId
Status DOCUMENT_PROFILE_PARSER_DEFINITION(algo, DocId) {
  const std::string& docid = document->docid();
  feature_proto::Feature feature;
  feature.set_v_string(docid);
  (*feature_map)[feature_conf.id] = std::move(feature);

  feature_proto::Feature hash_feature;
  hash_feature.set_v_uint64(MAKE_HASH(docid));
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  DocFeatureLog<std::string>(document, feature_conf, docid);
  return Status::OK();
}

// Feature:DocType
Status DOCUMENT_PROFILE_PARSER_DEFINITION(algo, DocType) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, skipType);
  const rapidjson::Value& doctype_value = json_doc["skipType"];
  if (!doctype_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid skipType type");
  }

  std::string doctype = doctype_value.GetString();
  feature_proto::Feature feature;
  feature.set_v_string(doctype);
  (*feature_map)[feature_conf.id] = std::move(feature);

  feature_proto::Feature hash_feature;
  hash_feature.set_v_uint64(MAKE_HASH(doctype));
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  DocFeatureLog<std::string>(document, feature_conf, doctype);
  return Status::OK();
}

// Feature:DocCategory
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocCategory) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, category);
  const rapidjson::Value& category_value = json_doc["category"];
  if (!category_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid category type");
  }

  std::string category = category_value.GetString();
  std::vector<std::string> category_vector;
  // StringUtils::Split(category, ',', category_vector);

  std::string delimiters("|,/");
  std::vector<std::string> parts;
  boost::split(category_vector, category, boost::is_any_of(delimiters));

  if (category_vector.empty()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid category value");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  feature_proto::Feature& hash_feature
    = (*feature_map)[feature_conf.id + kHashFeatureOffset];
  feature_proto::ListUint64* list_hash = hash_feature.mutable_v_list_uint64();

  std::string v;
  for (const std::string& c : category_vector) {
    v += c;
    list_hash->add_k(MAKE_HASH(v));
    list_string->add_k(v);
    DocFeatureLog<std::string>(document, feature_conf, v);
  }
  return Status::OK();
}

// Feature:DocSource
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocSource) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, source);
  const rapidjson::Value& json_value = json_doc["source"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid origin type");
  }

  UPDATE_STRING_FEATURE(json_value);
  UPDATE_STRING_FEATURE_HASH(json_value);
  return Status::OK();
}

// Feature:DocSourceLevel
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocSourceLevel) {
  if (!json_doc.HasMember("sourceLevel")) {
    return Status(error::DOCUMENT_PARSE_ERROR, "no sourceLevel for DocSourceLevel");
  }
  const rapidjson::Value& json_value = json_doc["sourceLevel"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid sourceLevel type");
  }

  UPDATE_STRING_FEATURE(json_value);
  UPDATE_STRING_FEATURE_HASH(json_value);
  return Status::OK();
}

// Feature:DocPublishTime
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocPublishTime) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, ptime);
  if (!json_doc.HasMember("ptime")) {
    return Status(error::DOCUMENT_PARSE_ERROR, "no ptime");
  }
  const rapidjson::Value& json_value = json_doc["ptime"];
  if (!json_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid ptime type");
  }

  // 单位为秒
  int64_t publish_time = TimeUtils::DateToTimestamp(json_value.GetString());

  feature_proto::Feature feature;
  feature.set_v_int64(publish_time);
  (*feature_map)[feature_conf.id] = std::move(feature);
  FeatureLog<int64_t>(feature_conf, document, publish_time);
  return Status::OK();
}

// Feature:DocManuallyInterest
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocManuallyInterest) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, interests);
  const rapidjson::Value& interest_value = json_doc["interests"];
  if (!interest_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid interests type");
  }

  std::string interests = interest_value.GetString();
  std::vector<std::string> interests_vector;
  StringUtils::Split(interests, ',', interests_vector);
  if (interests_vector.empty()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid interests value");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  feature_proto::Feature& feature_hash
    = (*feature_map)[feature_conf.id + kHashFeatureOffset];
  feature_proto::ListUint64* list_hash = feature_hash.mutable_v_list_uint64();

  for (std::string& v : interests_vector) {
    StringUtils::ReplaceAll(v, "POI", "");
    list_hash->add_k(MAKE_HASH(v));
    list_string->add_k(v);
    DocFeatureLog<std::string>(document, feature_conf, v);
  }
  return Status::OK();
}

// Feature:DocManuallyInterestSplit
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocManuallyInterestSplit) {
  if (!json_doc.HasMember("interests")) {
    return Status(error::DOCUMENT_PARSE_ERROR, "no interests for DocManuallyInterest");
  }
  const rapidjson::Value& interest_value = json_doc["interests"];
  if (!interest_value.IsString()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid interests type");
  }

  std::string interests = interest_value.GetString();
  std::vector<std::string> interests_vector;
  StringUtils::Split(interests, ',', interests_vector);
  if (interests_vector.empty()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid interests value");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  feature_proto::Feature& feature_hash
    = (*feature_map)[feature_conf.id + kHashFeatureOffset];
  feature_proto::ListUint64* list_hash = feature_hash.mutable_v_list_uint64();

  std::unordered_set<std::string> interests_set;
  for (std::string& v : interests_vector) {
    StringUtils::ReplaceAll(v, "POI", "");
    interests_set.insert(v);
    if (v.find('_') != std::string::npos) {
      std::vector<std::string> sub_interests;
      commonlib::StringUtils::Split(v, '_', sub_interests);
      for (const std::string& sub_interest : sub_interests) {
        if (sub_interest.empty()) {
          continue;
        }
        interests_set.insert(sub_interest);
      }
    }
  }

  for (const std::string& v : interests_set) {
    list_hash->add_k(MAKE_HASH(v));
    list_string->add_k(v);
    DocFeatureLog<std::string>(document, feature_conf, v);
  }

  return Status::OK();
}

// Feature:DocW2vTopic
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocW2vTopic) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, w2v_topic);

  const rapidjson::Value& topic_w2v = json_doc["w2v_topic"];
  // topic_w2v is an array
  if (!topic_w2v.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid topic_w2v type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  feature_proto::Feature& feature_hash
    = (*feature_map)[feature_conf.id + kHashFeatureOffset];
  feature_proto::ListUint64* list_hash = feature_hash.mutable_v_list_uint64();

  for (int i = 0; i < topic_w2v.Size(); i++) {
    const rapidjson::Value& sub_topic_w2v = topic_w2v[i];

    if (sub_topic_w2v.HasMember("topic_id")) {
      const std::string &v = sub_topic_w2v["topic_id"].GetString();
      DocFeatureLog<std::string>(document, feature_conf, v);
      list_hash->add_k(MAKE_HASH(v));
      list_string->add_k(std::move(v));
    }
  }
  return Status::OK();
}

// Feature:DocTopicKeyword
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocTitleKeyword) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, title_kw);

  const rapidjson::Value& title_kw = json_doc["title_kw"];
  // title_kw is an array
  if (!title_kw.IsArray()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid title_kw type");
  }

  feature_proto::Feature& feature = (*feature_map)[feature_conf.id];
  feature_proto::ListString* list_string = feature.mutable_v_list_string();

  feature_proto::Feature& hash_feature
    = (*feature_map)[feature_conf.id + kHashFeatureOffset];
  feature_proto::ListUint64* list_hash = hash_feature.mutable_v_list_uint64();

  for (int i = 0; i < title_kw.Size(); i++) {
    const rapidjson::Value& sub_title_kw = title_kw[i];

    if (sub_title_kw.HasMember("word")) {
      const std::string& v = sub_title_kw["word"].GetString();
      list_hash->add_k(MAKE_HASH(v));
      list_string->add_k(v);
      DocFeatureLog<std::string>(document, feature_conf, v);
    }
  }

  return Status::OK();
}

// Feature:DocIsSantu
Status ALGO_DOCUMENT_PROFILE_PARSER_DEFINITION(DocIsSantu) {
  CHECK_JSON_ITEM_EXISTS_OR_RETURN(json_doc, is_santu);
  const rapidjson::Value& json_value = json_doc["is_santu"];
  if (!json_value.IsInt()) {
    return Status(error::DOCUMENT_PARSE_ERROR, "Invalid is_santu type");
  }
  std::string value = std::to_string(json_value.GetInt());
  feature_proto::Feature feature;
  feature.set_v_string(value);
  (*feature_map)[feature_conf.id] = std::move(feature);
  feature_proto::Feature hash_feature;
  hash_feature.set_v_uint64(MAKE_HASH(value));
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  DocFeatureLog<std::string>(document, feature_conf, value);
  return Status::OK();
}

}  // namespace feature_engine
