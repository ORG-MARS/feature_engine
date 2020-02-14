// File   main.cpp
// Author lidongming
// Date   2018-09-04 18:25:11
// Brief

#include <map>
#include <thread>
#include <chrono>
#include <iostream>
#include <unordered_map>

#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/gflags/include/gflags/gflags.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/deps/commonlib/include/redis_cluster_client.h"
#include "feature_engine/deps/commonlib/include/monitor.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"

#include "feature_engine/index/index_entry.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/server/feature_server.h"
#include "feature_engine/feature/feature_conf_parser.h"
#include "feature_engine/reloader/data_manager.h"

#include "feature_engine/util/serman_util.h"

using namespace feature_engine;

// Reload gflags
std::unique_ptr<gflags::FlagSaver> current_flags(new gflags::FlagSaver());
void StartConfigReloadThread() {
    std::thread([]() {
        while (1) {
            current_flags.reset();
            current_flags.reset(new gflags::FlagSaver());
            gflags::ReparseCommandLineNonHelpFlags();
            // Reload conf/gflags.conf per 10s
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }).detach();
}

int main(int argc, char** argv) {
  // Init gflags
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  //gflags::SetCommandLineOption("flagfile", "./conf/gflags.conf");
  StartConfigReloadThread();

  // Init glog
  google::InitGoogleLogging(argv[0]);
  FLAGS_log_dir = "./logs";
  google::FlushLogFiles(google::WARNING);
  FLAGS_logbufsecs = 0;

  LOG(INFO) << "start feature engine";
  
  // Init monitor
  commonlib::monitor::Monitor::GetInstance().Init(FLAGS_monitor_data, FLAGS_monitor_status,
                              FLAGS_monitor_interval);
  commonlib::monitor::Monitor::GetInstance().Start();
  LOG(INFO) << "init monitor successfully";

  // Init DataManager
  reloader::DataManager& data_manager = reloader::DataManager::Instance();
  data_manager.Init();

  std::shared_ptr<std::unordered_set<std::string>> docs = data_manager.GetDocDB();
  if (docs != nullptr) {
    for (const std::string& docid : *docs) {
      LOG(INFO) << "reserved doc:" << docid;
    }
  }

  // Init IndexEntry
  IndexEntry& index_entry = IndexEntry::GetInstance();
  index_entry.Init();

  // Parse FeatureConf
  FeatureConfParser& feature_conf_parser = FeatureConfParser::GetInstance();
  if (feature_conf_parser.parse_status() != Status::OK()) {
    LOG(FATAL) << "parse feature conf error:" << FLAGS_feature_conf_path;
    return -1;
  }

  int server_thread_count = std::thread::hardware_concurrency();
  // Init feature server
  FeatureServer server(server_thread_count, FLAGS_port);
  if (!server.Init()) {
    LOG(FATAL) << "init feature server error";
    return -1;
  }

  // register consul
  if (!SermanUtils::regServer()) {
    //return -1 ;
  }

  commonlib::TimeUtils::setJetLag();

  // Start feature server
  server.Start();

#if 0 
  // Pause program
  std::string dummy;
  std::cout << "Enter to continue..." << std::endl;
  std::getline(std::cin, dummy);
#endif

  return 0;
}
