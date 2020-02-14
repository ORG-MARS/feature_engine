// File   doc_consumer.cpp
// Author lidongming
// Date   2018-09-05 17:00:46
// Brief

#include "feature_engine/index/doc_consumer.h"
// #include <fstream>
// #include <iostream>
#include <thread>
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/commonlib/include/util.h"
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/deps/commonlib/include/monitor.h"
#include "feature_engine/index/index_entry.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

using namespace commonlib;

DocConsumer::DocConsumer(int partition, IndexEntry* index_entry)
  : KafkaConsumer(partition),
  index_entry_(index_entry) {
}

DocConsumer::~DocConsumer() { }

void DocConsumer::Start() {
  Init();
  LOG(INFO) << "start doc consumer partition:" << partition_;

  std::thread(&DocConsumer::Consume, this).detach();
}

void DocConsumer::ProcessMsg(const RdKafka::Message* message) {
  monitor::Monitor::inc("docs_count_from_kafka", 1);
  // Init document from json consumed
  std::string msg(reinterpret_cast<char*>(message->payload()), message->len());
  std::shared_ptr<Document> document = std::make_shared<Document>();
  Status status = document->ParseFromJson(msg);
  document->Init();

  if (status.ok()) {
    Status valid_status = document->IsValid();
    if (valid_status.ok()) {
      UpdateDocument(document);
      monitor::Monitor::inc("valid_docs_count_from_kafka", 1);
    } else {
      monitor::Monitor::inc("invalid_docs_count_from_kafka", 1);
      // LOG(INFO) << "skip document docid:" << document->docid()
      //           << " error:" << valid_status;
    }
  } else {
    monitor::Monitor::inc("invalid_docs_count_from_kafka", 1);
    LOG(WARNING) << "parse document error:" << status;
  }
}

int DocConsumer::UpdateDocument(std::shared_ptr<Document>& document) {
  index_entry_->UpdateDocument(document);
  // if (FLAGS_debug) {
  // LOG(INFO) << "receive valid document from kafka docid:" << document->docid();
  // }
  return 0;
}

}  // namespace feature_engine
