// File   kafka_consumer.cpp
// Author lidongming
// Date   2018-09-05 16:23:23
// Brief

#include "feature_engine/index/kafka_consumer.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

KafkaConsumer::KafkaConsumer(int partition) :
    consumer_(NULL), topic_(NULL),
    run_(true), partition_(partition), batch_cnt_(0) {
}

KafkaConsumer::~KafkaConsumer() {
    if (consumer_ != NULL) {
        consumer_->stop(topic_, partition_);
        consumer_->poll(1000);
        delete consumer_;
        consumer_ = NULL;
    }
    if (topic_ != NULL) {
        delete topic_;
        topic_ = NULL;
    }
}

int KafkaConsumer::Init() {
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    std::string errstr;

    // Set kafka compression
    if (!FLAGS_compression.empty()) {
        if (conf->set("compression.codec", FLAGS_compression, errstr)
            != RdKafka::Conf::CONF_OK) {
            LOG(WARNING) << "set kafka compression failed:"
                         << FLAGS_compression;
        }
    }

    // Set offset config
    if (!FLAGS_auto_commit.empty()) {
        if (tconf->set("auto.commit.enable", FLAGS_auto_commit, errstr)
            != RdKafka::Conf::CONF_OK) {
            LOG(WARNING) << "set kafka auto commit failed offset:"
                         << FLAGS_auto_commit << " error:" << errstr;
        }
    }
    if (!FLAGS_commit_interval.empty()) {
        if (tconf->set("auto.commit.interval.ms", FLAGS_commit_interval, errstr)
            != RdKafka::Conf::CONF_OK) {
            LOG(WARNING) << "set kafka commit interval failed interval:"
                         << FLAGS_commit_interval << " error:" << errstr;
        }
    }
    
#if 0
    if (!FLAGS_store_method.empty()) {
        if (tconf->set("offset.store.method", FLAGS_store_method, errstr)
            != RdKafka::Conf::CONF_OK) {
            LOG(WARNING) << "set kafka offset store method failed method:"
                         << FLAGS_store_method << " error:" << errstr;
        }
    }

    tconf->set("offset.store.path", FLAGS_offset_store, errstr);
#endif

    // Set kafka broker
    if (!FLAGS_broker.empty()) {
        if (conf->set("metadata.broker.list", FLAGS_broker, errstr)
            != RdKafka::Conf::CONF_OK) {
            LOG(WARNING) << "set kafka broker failed broker:" <<  FLAGS_broker;
        }
    }

    // Create consumer
    consumer_ = RdKafka::Consumer::create(conf, errstr);
    if (!consumer_) {
      LOG(FATAL) << "Failed to create consumer error:" << errstr;
      return -1;
    }

    // Set kafka topic 
    topic_ = RdKafka::Topic::create(consumer_, FLAGS_topic, tconf, errstr);
    if (!topic_) {
        LOG(FATAL) << "Failed to create topic error:" << errstr;
        delete tconf;
        return -1;
    } else {
        delete tconf;
        LOG(INFO) << "create topic:" << FLAGS_topic;
    }

    // Set kafka offset
    int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
    if (FLAGS_offset_type == "end") {
        start_offset = RdKafka::Topic::OFFSET_END;
    } else if (FLAGS_offset_type == "beginning") {
        start_offset = RdKafka::Topic::OFFSET_BEGINNING;
    } else if (FLAGS_offset_type == "stored") {
        start_offset = RdKafka::Topic::OFFSET_STORED;
    } else {
        start_offset = FLAGS_offset;
    }

    RdKafka::ErrorCode resp = consumer_->start(topic_, partition_, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR) {
        LOG(FATAL) << "Failed to start consumer:" << RdKafka::err2str(resp);
        return -1;
    }

    return 0;
}

void KafkaConsumer::MsgConsumer(RdKafka::Message* message, void* opaque) {
    switch (message->err()) {
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            break;
        case RdKafka::ERR_NO_ERROR:
            ProcessMsg(message);
            break;
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            LOG(WARNING) << "Consume failed:" << message->errstr();
            break;
        default:
            LOG(WARNING) << "Consume failed:" << message->errstr();
            break;
    }
}

void KafkaConsumer::Consume() {
    while (run_) {
        RdKafka::Message* msg = consumer_->consume(topic_, partition_, 1000); 
        MsgConsumer(msg, NULL);
        delete msg;
        consumer_->poll(0);
        
        ++batch_cnt_;
        if (batch_cnt_ > 1000) {
            batch_cnt_ = 0;
            usleep((rand() & 1023) * 1000);
        }
    }
}

}  // namespace feature_engine
