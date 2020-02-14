// File   parser_manager.h
// Author lidongming
// Date   2018-09-06 08:55:45
// Brief

#ifndef FEATURE_ENGINE_PARSER_PARSER_MANAGER_H_
#define FEATURE_ENGINE_PARSER_PARSER_MANAGER_H_

#include <map>
#include <string>
#include "feature_engine/deps/commonlib/include/status.h"
// #include "feature_engine/parser/feature_cache.h"
#include "feature_engine/parser/user_profile_parser.h"
#include "feature_engine/parser/document_profile_parser.h"
#include "feature_engine/parser/manual_method_parser.h"
#include "feature_engine/feature/feature_conf.h"

namespace feature_engine {

using commonlib::Status;

class Document;
class ParserManager {
 public:
  static ParserManager& GetInstance() {
    static ParserManager instance;
    return instance;
  }

  ParserManager();
  // ~ParserManager();

  void Init();

  const std::map<std::string, DocumentProfileParserFunc>& document_parser_map() {
    return document_profile_parser_.parser_map();
  }

  const std::map<std::string, UserProfileParserFunc>& up_parser_map() {
    return user_profile_parser_.parser_map();
  }

  const std::map<std::string, ManualMethodParserFunc>& manual_parser_map() {
    return manual_method_parser_.parser_map();
  }

 private:
  DocumentProfileParser document_profile_parser_;
  ManualMethodParser manual_method_parser_;
  UserProfileParser user_profile_parser_;
};  // ParserManager

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_PARSER_MANAGER_H_
