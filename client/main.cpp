// File   main.cpp
// Author lidongming
// Date   2018-08-30 20:35:05
// Brief

#include <map>
#include <fstream>
#include <thread>
#include <random>
#include <algorithm>  //for std::generate_n
#include "feature_engine/deps/gflags/include/gflags/gflags.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/thrift/include/thrift/transport/TSocket.h"
#include "feature_engine/deps/thrift/include/thrift/transport/TBufferTransports.h"
#include "feature_engine/deps/thrift/include/thrift/protocol/TBinaryProtocol.h"
#include "feature_engine/deps/protobuf/include/google/protobuf/text_format.h"
#include "feature_engine/deps/commonlib/include/string_utils.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/ml-thrift/gen-cpp/FeatureService.h"

using namespace commonlib;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::concurrency;
using namespace feature_thrift;

DEFINE_string(client_type, "single", "client type");
DEFINE_string(server_ip, "127.0.0.1", "server ip");
DEFINE_int32(server_port, 12359, "server port");
DEFINE_int32(client_threads_count, 10, "client threads count");
DEFINE_int32(doc_count, 1000, "sample count");
DEFINE_string(search_docid, "", "");

DEFINE_string(docs_file, "./data/docs.dat", "");
DEFINE_string(feature_ids_file, "./data/feature_ids.dat", "");

std::string random_string(size_t length) {
  auto randchar = []() -> char
  {
          const char charset[] =
          "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
          const size_t max_index = (sizeof(charset) - 1);
          return charset[rand() % max_index];
  };
  std::string str(length,0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

int ReadDocsFromFile(const std::string& docs_file,
    // std::vector<std::string>* docs) {
    std::vector<common_ml_thrift::DocInfo>* docs) {
  std::ifstream ifs(docs_file);
  if (!ifs.is_open()) {
    LOG(INFO) << "read docs from local file error";
    return -1;
  }
  std::string line;
  while (std::getline(ifs, line)) {
    common_ml_thrift::DocInfo docinfo;
    docinfo.docid = line;
    docs->emplace_back(std::move(docinfo));
  }
  LOG(INFO) << "docs count:" << docs->size();
  return 0;
}

int ReadFeatureIdsFromFile(const std::string& file_name,
    std::vector<int>* ids) {
  std::ifstream ifs(file_name);
  if (!ifs.is_open()) {
    LOG(INFO) << "read feature ids from local file error";
    return -1;
  }
  std::string line;
  while (std::getline(ifs, line)) {
    ids->emplace_back(atoi(line.c_str()));
  }
  LOG(INFO) << "feature idscount:" << ids->size();
  return 0;
}

int StartClient(bool loop) {
  // Start client
  boost::shared_ptr<TSocket> socket(new TSocket(FLAGS_server_ip, FLAGS_server_port));
  boost::shared_ptr<TFramedTransport> transport(new TFramedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  std::shared_ptr<FeatureServiceClient> service_client(
      new FeatureServiceClient(protocol));
  transport->open();

  feature_thrift::FeatureRequest request;

  TimeRecorder recorder;
  feature_thrift::FeatureResponse response;

  ReadDocsFromFile(FLAGS_docs_file, &request.docs);
  ReadFeatureIdsFromFile(FLAGS_feature_ids_file, &request.feature_ids);

  do {
    request.rid = random_string(16);
    // Call feature
    recorder.StartTimer("feature_latency");
    service_client->Features(response, request);
    recorder.StopTimer("feature_latency");

    LOG(INFO) << "finish feature rid:" << request.rid
      << " latency:" << recorder.GetElapse("feature_latency");

    // feature_proto::FeatureList feature_list;
    // feature_list.ParseFromString(response.serialized_features);
    // std::string v;
    // google::protobuf::TextFormat::PrintToString(feature_list, &v);
    // LOG(INFO) << "feature_value:" << v;

    for (const auto& feature_value : response.serialized_features) {
      feature_proto::Features features;
      features.ParseFromString(feature_value);
      std::string v;
      google::protobuf::TextFormat::PrintToString(features, &v);
      LOG(INFO) << "features:" << v;
    }
  } while(loop);
  return 0;
}

void ClientThread(bool loop) {
  while (true) {
    StartClient(loop);
  }
}

void StressTest() {
  std::vector<std::thread> threads;
  threads.resize(FLAGS_client_threads_count);
  for (int i = 0; i < FLAGS_client_threads_count; i++) {
    threads[i] = std::thread(&ClientThread, true);
  }
  for (auto& t : threads) {
    t.join();
  }
}

int SearchDoc() {
  boost::shared_ptr<TSocket> socket(new TSocket(FLAGS_server_ip, FLAGS_server_port));
  boost::shared_ptr<TFramedTransport> transport(new TFramedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  std::shared_ptr<FeatureServiceClient> service_client(
      new FeatureServiceClient(protocol));
  transport->open();

  feature_thrift::SearchDocRequest request;
  request.rid = random_string(16);
  request.docid = FLAGS_search_docid;
  request.feature_ids.emplace_back(1);

  feature_thrift::SearchDocResponse response;

  service_client->SearchDoc(response, request);
  LOG(INFO) << "rid:" << request.rid << " response:" << response;
  return 0;
}

int main(int argc, char**argv) {
  // Init gflags
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  gflags::SetCommandLineOption("flagfile", "./conf/client.conf");

  // Init glog
  google::InitGoogleLogging(argv[0]);
  FLAGS_log_dir = "./logs";
  google::FlushLogFiles(google::WARNING);
  FLAGS_logbufsecs = 0;

  srand(time(NULL));
  if (FLAGS_client_type == "single") {
    // Single request
    StartClient(false);
  } else if (FLAGS_client_type == "loop") {
    // Loop request
    ClientThread(false);
  } else if (FLAGS_client_type == "multi") {
    // Stress test
    StressTest();
  } else if (FLAGS_client_type == "search") {
    SearchDoc();
  }

  return 0;
}
