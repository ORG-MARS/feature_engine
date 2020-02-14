// File   feature_define.h
// Author lidongming
// Date   2018-09-05 23:11:43
// Brief

#ifndef FEATURE_ENGINE_FEATURE_DEFINE_H_
#define FEATURE_ENGINE_FEATURE_DEFINE_H_

// #include <string>

namespace feature_engine {

#if 0
// Basic feature name
extern const std::string kFeatureDocID;
extern const std::string kFeatureTopicW2V;
extern const std::string kFeatureExpireTime;
#endif

// FIXME(lidongming):make required
extern const int kFeatureDocID;
extern const int kFeatureDocIDHash;
extern const int kFeatureExpireTime;

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_FEATURE_DEFINE_H_
