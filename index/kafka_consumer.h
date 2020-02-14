// File   kafka_consumer.h
// Author lidongming
// Date   2018-09-05 16:23:02
// Brief

#ifndef FEATURE_ENGINE_INDEX_KAFKA_CONSUMER_H_
#define FEATURE_ENGINE_INDEX_KAFKA_CONSUMER_H_

#include <string>
#include "feature_engine/deps/librdkafka/include/librdkafka/rdkafkacpp.h"

namespace feature_engine {

class KafkaConsumer {
 public:
  KafkaConsumer(int partition);
  virtual ~KafkaConsumer();
  int Init();
  void Consume();
  void Stop() { run_ = false; }

  RdKafka::Topic* GetTopic() { return topic_; }
  RdKafka::Consumer* GetConsumer() { return consumer_; }
  int32_t GetPartition() { return partition_; }

 protected:
  RdKafka::Consumer* consumer_;
  RdKafka::Topic* topic_;
  bool run_;
  int32_t partition_;
  uint32_t batch_cnt_;
  virtual void ProcessMsg(const RdKafka::Message* message) = 0;
  void MsgConsumer(RdKafka::Message* message, void* opaque); 

};  // KafkaConsumer

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_KAFKA_CONSUMER_H_
