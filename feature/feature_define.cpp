// File   feature_define.cpp
// Author lidongming
// Date   2018-09-05 23:17:50
// Brief

#include "feature_engine/feature/feature_define.h"

namespace feature_engine {

#if 0
// FIXME(lidongming):should use feature conf instead
const std::string kFeatureDocID = "docid";
const std::string kFeatureTopicW2V = "topic_w2v";
const std::string kFeatureExpireTime = "expire_time";
#endif

const int kFeatureDocID = 1;
const int kFeatureDocIDHash = 2;
const int kFeatureExpireTime = 3;

}  // namespace feature_engine
