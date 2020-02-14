// File   ufs_client.cpp
// Author lidongming
// Date   2018-09-20 18:36:27
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
//#include "feature_engine/thrift/gen-cpp/common_types.h"
//#include "feature_engine/thrift/gen-cpp/UserProfileService.h"

using namespace commonlib;

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
//using namespace ::apache::thrift::concurrency;
// using namespace rec;

//using LabelType = rec::LABEL_TYPE::type;

DEFINE_string(ufs_server_ip, "10.200.129.170", "server ip");
DEFINE_int32(ufs_server_port, 8019, "server port");

/*int StartClient() {
  // Start client
  boost::shared_ptr<TSocket> socket(
      new TSocket(FLAGS_ufs_server_ip, FLAGS_ufs_server_port));
  boost::shared_ptr<TFramedTransport> transport(new TFramedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  std::shared_ptr<rec::UserProfileServiceClient> service_client(
      new rec::UserProfileServiceClient(protocol));
  transport->open();

  rec::UserProfileServiceRequest request;
  rec::UserProfileServiceResponse response;

  std::string sid = "123456";
  std::string page_id = "toutiao";
  std::string deviceid = "A8435165-7716-449B-8300-58F9D07BC736";

  request.__set_sid(sid);
  request.__set_search_type(rec::SEARCH_TYPE::type::NEWS_APP_RECOMMEND);

  rec::UserRequest user_request;
  user_request.__set_uid_type(rec::UID_TYPE::type::DEVICE_ID);
  user_request.__set_uid("CQk2OWI0ODAwYzk4MDhkMTM1CVMyNVFCRFBEMjJDUlY");

  rec::DeviceID device_id;
  device_id.__set_uuid(deviceid);
  rec::Device device;
  device.__set_device_id(device_id);
  user_request.__set_device(device);

  rec::Network network;
  network.__set_ipv4("127.0.0.1");
  user_request.__set_network(network);

  // rec::Gps gps;
  // gps.__set_gps_type(gps_type);
  // gps.__set_longitude(lng);
  // gps.__set_latitude(lat);
  // user_request.__set_gps(gps);

  rec::PageRequest page_request;
  page_request.__set_page_id(page_id);

  rec::UIRequest ui_request;
  ui_request.__set_search_type(rec::SEARCH_TYPE::type::NEWS_APP_RECOMMEND);
  ui_request.__set_sid(sid);

  ui_request.__set_user_request(user_request);
  ui_request.__set_page_request(page_request);

  request.__set_ui_request(ui_request);

  service_client->search(response, request);

  LOG(INFO) << "response err_code:" << response.err_code
      << " msg:" << response.msg
      << " rt_labels_size:" << response.user_profile.rt_labels.size()
      << " gender:" << response.user_profile.gender
      << " age:" << response.user_profile.age
      << "";

  auto& labels = response.user_profile.rt_labels;
  auto it = labels.find(LabelType::W2V_CLUSTERING);
  if (it != labels.end()) {
      LOG(INFO) << "found topic";
    for (const rec::LabelInfo& l : it->second) {
      LOG(INFO) << "topic:" << l.label_id;
    }
  } else {
      LOG(INFO) << "no topic";
  }

  return 0;
}*/

int main(int argc, char**argv) {
  // Init gflags
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  gflags::SetCommandLineOption("flagfile", "./conf/client.conf");

  // Init glog
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
  // FLAGS_log_dir = "./logs";
  // google::FlushLogFiles(google::WARNING);
  // FLAGS_logbufsecs = 0;

  //StartClient();

  return 0;
}
