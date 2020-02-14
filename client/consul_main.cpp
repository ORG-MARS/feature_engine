#include <iostream>
#include "feature_engine/deps/gflags/include/gflags/gflags.h"
#include "feature_engine/util/serman_util.h"

using namespace google;
using namespace feature_engine;

int main(int argc, char** argv) {
  // Init gflags
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if(FLAGS_consul_client_mode == "deregister"){
    SermanUtils::deleteServer();
  }else if (FLAGS_consul_client_mode == "register"){
    SermanUtils::regServer() ;
  }else {
    std::cout << "mode error,must be in [register|deregister]" << std::endl;
  }
}
