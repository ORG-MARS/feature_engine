// File   parser_lib.h
// Author lidongming
// Date   2018-09-10 23:42:16
// Brief

#ifndef FEATURE_ENGINE_PARSER_PARSER_H_
#define FEATURE_ENGINE_PARSER_PARSER_H_

#include <string>
#include <vector>

namespace feature_engine {

class Parser {
 public:
  Parser() {
  }
  virtual ~Parser() {
  }

  virtual void parsers(std::vector<std::string>* parsers) const = 0;
  virtual void ReloadParsers() = 0;
  virtual void Register() = 0;

};  // Parser

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_PARSER_H_
