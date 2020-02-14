// File   feature_lib_register.cpp
// Author lidongming
// Date   2018-09-10 23:51:09
// Brief

#include "feature_engine/parser/document_profile_parser.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

namespace feature_engine {

using namespace commonlib;

#define REGISTER_PARSER(_parser_) \
  RegisterParser(#_parser_, _parser_);

void DocumentProfileParser::RegisterParser(std::string parser_name,
    DocumentProfileParserFunc parser) {
  parser_map_[parser_name] = std::move(parser);
}

// Register all feature functions
void DocumentProfileParser::Register() {
  // Required Features
  // required_features_.insert("base_docid");
  // required_features_.insert("base_docid_hash");
  // required_features_.insert("base_expire_time");

  REGISTER_PARSER(base_docid);
  REGISTER_PARSER(base_docid_hash);
  REGISTER_PARSER(base_expire_time);

  // Basic Features
  REGISTER_PARSER(ai_titleSegList);
  REGISTER_PARSER(ai_source_type);
  REGISTER_PARSER(ai_quality);
  REGISTER_PARSER(ai_site_id);
  REGISTER_PARSER(ai_accountOriginal);
  REGISTER_PARSER(ai_doc_byte_count);
  REGISTER_PARSER(ai_localcity);
  REGISTER_PARSER(ai_is_santu);
  REGISTER_PARSER(ai_doctype);
  REGISTER_PARSER(ai_origion);

  REGISTER_PARSER(ai_title_word_count);
  REGISTER_PARSER(ai_interests);
  REGISTER_PARSER(ai_source);
  REGISTER_PARSER(ai_title_char_count);
  REGISTER_PARSER(ai_doc_char_count);
  REGISTER_PARSER(ai_sansu_score);

  REGISTER_PARSER(ai_topic);
  REGISTER_PARSER(ai_accountCategory);
  REGISTER_PARSER(ai_sourceLevel);
  REGISTER_PARSER(ai_vulgar);

  REGISTER_PARSER(ai_dkeys);
  REGISTER_PARSER(ai_qualityLevel);
  REGISTER_PARSER(ai_category);
  REGISTER_PARSER(ai_topic_w2v);

  REGISTER_PARSER(ai_data_source);
  REGISTER_PARSER(ai_title_kw);
  REGISTER_PARSER(ai_title_kw_v2);
  REGISTER_PARSER(ai_articleEntity);

  REGISTER_PARSER(algo_DocId);
  REGISTER_PARSER(algo_DocType);
  REGISTER_PARSER(algo_DocCategory);
  REGISTER_PARSER(algo_DocSource);
  REGISTER_PARSER(algo_DocSourceLevel);
  REGISTER_PARSER(algo_DocPublishTime);
  REGISTER_PARSER(algo_DocManuallyInterest);
  REGISTER_PARSER(algo_DocManuallyInterestSplit);
  REGISTER_PARSER(algo_DocW2vTopic);
  REGISTER_PARSER(algo_DocTitleKeyword);
  REGISTER_PARSER(algo_DocIsSantu);
}

}  // namespace feature_engine
