// File   parser_manager.cpp
// Author lidongming
// Date   2018-09-06 09:25:27
// Brief

#include "feature_engine/parser/parser_manager.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/parser/user_profile.h"
#include "feature_engine/index/document.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/feature/feature_conf_parser.h"
#include "feature_engine/common/common_define.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

using namespace commonlib;

ParserManager::ParserManager() {
  Init();
}

void ParserManager::Init() {
  user_profile_parser_.Register();
  document_profile_parser_.Register();
  manual_method_parser_.Register();
}

}  // namespace feature_engine
