// File   parser_common.cpp
// Author lidongming
// Date   2018-09-18 23:53:31
// Brief

#include "feature_engine/parser/parser_common.h"

namespace feature_engine {

using namespace commonlib;

const std::string kMissing = "missing";
const int kHashFeatureOffset = 1000000;

// Default status
const Status feature_parse_error_status(error::FEATURE_PARSE_ERROR,
                                        "feature parse error");
const Status depend_not_found_status(error::FEATURE_PARSE_ERROR,
                                     "depend feature not found");
const Status invalid_type_status(error::FEATURE_PARSE_ERROR,
                                 "invalid hash depend type");
const Status invalid_param_status(error::FEATURE_PARSE_ERROR,
                                 "invalid params");
const Status invalid_doc_stat_status(error::FEATURE_PARSE_ERROR,
                                     "not exists in stat disc");
const Status not_found_doc_disc_item_status(error::FEATURE_PARSE_ERROR,
                                        "feature not found in doc_stat_disc");

}  // namespace feature_engine
