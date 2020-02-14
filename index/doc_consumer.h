// File   doc_consumer.h
// Author lidongming
// Date   2018-09-05 16:58:19
// Brief

#ifndef FEATURE_ENGINE_INDEX_DOC_KAFKA_CONSUMER_H_
#define FEATURE_ENGINE_INDEX_DOC_KAFKA_CONSUMER_H_

#include <set>
#include <map>
#include <memory>
#include "feature_engine/index/kafka_consumer.h"
#include "feature_engine/index/index_table.h"
#include "feature_engine/index/document.h"

namespace feature_engine {

class IndexEntry;

class DocConsumer : public KafkaConsumer {
public:
    DocConsumer(int partition, IndexEntry* index_entry);
    ~DocConsumer();

    void Start();

    void ProcessMsg(const RdKafka::Message* message) override;

    int UpdateDocument(std::shared_ptr<Document>& document);

    std::shared_ptr<Document> Json2Document(std::string& msg);

    IndexEntry* GetIndexEntry() { return index_entry_; }

private:
    IndexEntry* index_entry_;

};  // DocKafkaConsumer

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_DOC_KAFKA_CONSUMER_H_
