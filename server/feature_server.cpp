// Copyright 2018 Netease

// File   feature_engine.cpp
// Author lidongming
// Date   2018-09-11 01:25:10
// Brief

#include "feature_engine/server/feature_server.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/processor/TMultiplexedProcessor.h"

#include "thrift/concurrency/ThreadManager.h"
#include "thrift/concurrency/PosixThreadFactory.h"
// #include "thrift/transport/TNonblockingServerSocket.h"
#include "ml-thrift/gen-cpp/FeatureService.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/thread_pool.h"
#include "feature_engine/server/feature_handler.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using namespace feature_thrift;

namespace feature_engine {

FeatureServer::FeatureServer(int thread_count, int port)
    : thread_count_(thread_count), port_(port) {
}

FeatureServer::~FeatureServer() {  }

bool FeatureServer::Init() {
  LOG(INFO) << "init server thread_count:" << thread_count_
            << " port:" << port_;
  return true;
}

void FeatureServer::Start() {
  LOG(INFO) << "start feature server port:" << port_
            << " thread_count:" << thread_count_;

  // Init thrift server
  boost::shared_ptr<FeatureHandler> handler(new FeatureHandler());

  boost::shared_ptr<TProcessor> feature_service_processor(
    new feature_thrift::FeatureServiceProcessor(handler));

  boost::shared_ptr<TProcessor> doc_property_service_processor(
    new doc_property_thrift::DocPropertyServiceProcessor(handler));

  boost::shared_ptr<apache::thrift::TMultiplexedProcessor> processor(
      new apache::thrift::TMultiplexedProcessor());
  processor->registerProcessor("Feature Service",
                               feature_service_processor);
  processor->registerProcessor("DocProperty Service",
                               doc_property_service_processor);

  boost::shared_ptr<TProtocolFactory> protocol_factory(
      new TBinaryProtocolFactory());
  // std::shared_ptr<TNonblockingServerSocket> server_socket(
      // new TNonblockingServerSocket(port_));
  boost::shared_ptr<PosixThreadFactory> thread_factory(
      new PosixThreadFactory());
  boost::shared_ptr<ThreadManager> thread_manager =
      ThreadManager::newSimpleThreadManager(thread_count_);

  thread_manager->threadFactory(thread_factory);
  thread_manager->start();

  // Start thrift server
  try {
      // TNonblockingServer server(processor, protocol_factory, server_socket,
                                // thread_manager);
      TNonblockingServer server(processor, protocol_factory, port_,
                                thread_manager);
      server.setNumIOThreads(8);
      server.setOverloadAction(TOverloadAction::T_OVERLOAD_DRAIN_TASK_QUEUE);

#if 1
      server.setNumIOThreads(4);
      server.setOverloadAction(TOverloadAction::T_OVERLOAD_CLOSE_ON_ACCEPT);
      server.setConnectionStackLimit(600);
      server.setMaxConnections(600);
      server.setMaxActiveProcessors(600);
      server.setTaskExpireTime(50);
      server.setWriteBufferDefaultSize(1024 * 1024 * 10);
      server.setIdleReadBufferLimit(1024 * 1024 * 10);
      server.setIdleBufferMemLimit(1024 * 1024 * 10);
      server.setIdleWriteBufferLimit(1024 * 1024 * 10);
#endif

      server.serve();
  } catch (TException& tx) {
      LOG(FATAL) << "start feature server error:" << tx.what();
      exit(-1);
  }
}

}  // namespace feature_engine
