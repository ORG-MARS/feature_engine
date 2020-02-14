// File   feature_parser_lib.h
// Author lidongming
// Date   2018-09-06 09:09:07
// Brief

// Feature parser lib
// Main parser lib entry for features

#ifndef FEATURE_ENGINE_PARSER_DOCUMENT_PROFILE_PARSER_H_
#define FEATURE_ENGINE_PARSER_DOCUMENT_PROFILE_PARSER_H_

#include <functional>
#include <map>
#include <set>
#include "feature_engine/deps/rapidjson/document.h"
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/parser/parser.h"
#include "feature_engine/parser/parser_common.h"  // FeatureMap
#include "feature_engine/index/document.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/deps/commonlib/include/status.h"

namespace feature_engine {

using commonlib::Status;

using DocumentProfileParserFunc
  = std::function<Status(
      const rapidjson::Document&, const FeatureConf&, FeatureMap*, Document*)>;

class DocumentProfileParser : public Parser {
 public:
  #define DECLARE_DOCUMENT_PARSER(_group_, _parser_name_) \
  static Status _group_##_##_parser_name_(const rapidjson::Document& json_doc, \
      const FeatureConf& feature_conf, \
      FeatureMap* feature_map, Document* document)

  #define DECLARE_AI_DOCUMENT_PARSER(_parser_name_) \
   DECLARE_DOCUMENT_PARSER(ai, _parser_name_)
  #define DECLARE_ALGO_DOCUMENT_PARSER(_parser_name_) \
   DECLARE_DOCUMENT_PARSER(algo, _parser_name_)

 public:
  void parsers(std::vector<std::string>* parsers) const override {
    // FIXME(lidongming):IMPLEMENT
  }
  void ReloadParsers() override {
    // FIXME(lidongming):IMPLEMENT
  }

  void Register() override;

  const std::map<std::string, DocumentProfileParserFunc>& parser_map() const {
    return parser_map_;
  }

  // const std::set<int> required_features() const {
    // return required_features_;
  // }

 private:
  // WARNING:parser name shoule be the same as feature name
  void RegisterParser(std::string parser_name,
      DocumentProfileParserFunc parser);

 public:
  //----------------------------Required Feature Parsers----------------------//
  // docid
  DECLARE_DOCUMENT_PARSER(base, docid);
  // docid_hash
  DECLARE_DOCUMENT_PARSER(base, docid_hash);
  // expire_time
  DECLARE_DOCUMENT_PARSER(base, expire_time);

  //-----------------------------Base Feature Parsers-------------------------//
  //--------------------------------AI Features-------------------------------//
  // titleSegList
  DECLARE_AI_DOCUMENT_PARSER(titleSegList);
  // source_type
  DECLARE_AI_DOCUMENT_PARSER(source_type);
  // quality
  DECLARE_AI_DOCUMENT_PARSER(quality);
  // site_id
  DECLARE_AI_DOCUMENT_PARSER(site_id);
  // data_source
  DECLARE_AI_DOCUMENT_PARSER(data_source);
  // title_kw
  DECLARE_AI_DOCUMENT_PARSER(title_kw);
  DECLARE_AI_DOCUMENT_PARSER(title_kw_v2);
  DECLARE_AI_DOCUMENT_PARSER(articleEntity);
  // accountOriginal
  DECLARE_AI_DOCUMENT_PARSER(accountOriginal);
  DECLARE_AI_DOCUMENT_PARSER(doc_byte_count);
  DECLARE_AI_DOCUMENT_PARSER(localcity);
  DECLARE_AI_DOCUMENT_PARSER(is_santu);
  DECLARE_AI_DOCUMENT_PARSER(doctype);
  DECLARE_AI_DOCUMENT_PARSER(origion);
  DECLARE_AI_DOCUMENT_PARSER(title_word_count);
  DECLARE_AI_DOCUMENT_PARSER(interests);
  DECLARE_AI_DOCUMENT_PARSER(source);
  DECLARE_AI_DOCUMENT_PARSER(title_char_count);
  DECLARE_AI_DOCUMENT_PARSER(doc_char_count);
  DECLARE_AI_DOCUMENT_PARSER(sansu_score);
  DECLARE_AI_DOCUMENT_PARSER(topic);
  DECLARE_AI_DOCUMENT_PARSER(accountCategory);
  DECLARE_AI_DOCUMENT_PARSER(sourceLevel);
  DECLARE_AI_DOCUMENT_PARSER(vulgar);
  DECLARE_AI_DOCUMENT_PARSER(dkeys);
  DECLARE_AI_DOCUMENT_PARSER(qualityLevel);
  DECLARE_AI_DOCUMENT_PARSER(category);

  // topic_w2v
  DECLARE_AI_DOCUMENT_PARSER(topic_w2v);

  //-------------------------------Algo Features------------------------------//
  DECLARE_ALGO_DOCUMENT_PARSER(DocId);
  DECLARE_ALGO_DOCUMENT_PARSER(DocType);
  DECLARE_ALGO_DOCUMENT_PARSER(DocCategory);
  DECLARE_ALGO_DOCUMENT_PARSER(DocSource);
  DECLARE_ALGO_DOCUMENT_PARSER(DocSourceLevel);
  DECLARE_ALGO_DOCUMENT_PARSER(DocPublishTime);
  DECLARE_ALGO_DOCUMENT_PARSER(DocManuallyInterest);
  DECLARE_ALGO_DOCUMENT_PARSER(DocManuallyInterestSplit);
  DECLARE_ALGO_DOCUMENT_PARSER(DocW2vTopic);
  DECLARE_ALGO_DOCUMENT_PARSER(DocTitleKeyword);
  DECLARE_ALGO_DOCUMENT_PARSER(DocIsSantu);

 private:
  std::map<std::string, DocumentProfileParserFunc> parser_map_;
  // std::set<int> required_features_;
};  // DocumentProfileParser

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_DOCUMENT_PROFILE_PARSER_H_
