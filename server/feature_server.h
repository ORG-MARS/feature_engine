// File   feature_server.h
// Author lidongming
// Date   2018-08-28 15:06:37
// Brief

#ifndef FEATURE_ENGINE_SERVER_FEATURE_SERVER_H_
#define FEATURE_ENGINE_SERVER_FEATURE_SERVER_H_

#include <string>

namespace feature_engine {

class FeatureServer {
 public:
  FeatureServer(int thread_count, int port);
  ~FeatureServer();
		
  bool Init();
  void Start();

 private:
  int thread_count_;
  int port_;
};  // FeatureServer

}  // namespace feature

#endif  // FEATURE_ENGINE_SERVER_FEATURE_SERVER_H_

