// File   missing_manager.h
// Author lidongming
// Date   2019-03-05 17:28:09
// Brief

#ifndef FEATURE_ENGINE_INDEX_MISSING_MANAGER_H_
#define FEATURE_ENGINE_INDEX_MISSING_MANAGER_H_

#include "util/cache.h"
#include "index/index_entry.h"

namespace feature_engine {
 
// 请求处理线程在对应的queue(单生产者单消费者队列)中无锁新增元素,
// MissingManager类与MissingDocStatManager类从所有的queue中消费元素，
// 用于异步更新missing的数据，比如doc，docstat等数据结构.
// Template Arguments:
//  T:元素类型，如CacheItem
//  QUEUE_CAPACITY:queue的容量大小
template <typename T, int QUEUE_CAPACITY>
class MissingManager : public TLSQueueManager<T, QUEUE_CAPACITY> {
 public:
  MissingManager(const std::string& name,
                 uint32_t items_set_size,
                 uint32_t tls_queue_update_size,
                 uint32_t tls_queue_update_interval)
   : TLSQueueManager<T, QUEUE_CAPACITY>(name, items_set_size,
                                        tls_queue_update_size,
                                        tls_queue_update_interval) {
  }

  void RefreshProcess(const std::unordered_set<T>& items_set) override {
    if (!FLAGS_update_missing)
        return;
    namespace http = boost::beast::http;    // from <boost/beast/http.hpp>
    IndexEntry& index_entry = IndexEntry::GetInstance();

    int batch = 0;
    int doc_count = 0; // each 50 docs set a http request
    int total_count = 0; 
    std::vector<std::string> docids_vec;
    int missing_doc_size = items_set.size();
    HttpClient http_client(FLAGS_http_server_host, FLAGS_http_server_port, FLAGS_http_server_target, 11); // version:11
    int connect_ret = http_client.connect();
    if (connect_ret == 0) {
      for (const T& item: items_set) {
        ++total_count;
        const std::string& docid = item.id;
        if (docid.empty()) { continue; }

        if (doc_count < FLAGS_http_docid_count_limit) {
          docids_vec.emplace_back(std::move(docid));
          ++doc_count;
        }
        if (doc_count == FLAGS_http_docid_count_limit || total_count == missing_doc_size) {
          std::string req_json_body;
          std::string rid = random_string(16);
          http_client.getHttpBody(rid, docids_vec, req_json_body);
          DLOG(INFO) << "req json body is:" <<  req_json_body;

          http_client.setHttpReq(req_json_body);
          http_client.sentHttpReq();
          http::response<http::dynamic_body> res = http_client.getHttpRes();

          int parse_res = index_entry.ParseHttpRes(docids_vec, res);
          docids_vec.clear();
          doc_count = 0;
        } 
        ++batch;
        if (batch > 1000) {
          batch = 0;
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }
      LOG(INFO) << "update missing docs size:" << items_set.size();
    } else {
      LOG(WARNING) << "connect http error !, err code is:" << connect_ret;
    }

  }

private:
  DISALLOW_COPY_AND_ASSIGN(MissingManager);
  IndexBuilder index_builder_;
};

template <typename T, int QUEUE_CAPACITY = 1000>
class MissingDocStatManager : public TLSQueueManager<T, QUEUE_CAPACITY> {
 public:
  MissingDocStatManager(const std::string& name,
                        uint32_t items_set_size,
                        uint32_t tls_queue_update_size,
                        uint32_t tls_queue_update_interval)
   : TLSQueueManager<T, QUEUE_CAPACITY>(name, items_set_size, tls_queue_update_size,
                        tls_queue_update_interval) {
  }

  void RefreshProcess(const std::unordered_set<T>& items_set) override {
    RedisAdapter redis_client(FLAGS_redis_host, FLAGS_redis_port,
                              FLAGS_redis_password, FLAGS_redis_timeout_ms);

    if (items_set.empty()) { return; }

    IndexEntry& index_entry = IndexEntry::GetInstance();
    st::VersionManager<DocStatIndexTable>& doc_stat_table_vm
      = index_entry.doc_stat_table_vm();

    std::vector<std::string> docids;
    for (const T& item : items_set) {
      docids.emplace_back(item.id);
    }
    if (docids.empty()) {
      return;
    }

    int version = doc_stat_table_vm.create_version();
    if (version >= 0) {
      doc_stat_table_vm[version].Refresh();
      if (doc_stat_table_vm[version].Update(docids) != 0) {
        doc_stat_table_vm.drop_version(version);
      } else {
        if (doc_stat_table_vm.freeze_version(version) != 0) {
          doc_stat_table_vm.drop_version(version);
        } else {
          LOG(INFO) << "doc stats size:" << doc_stat_table_vm[version].size()
                    << " version:" << version;
        }
      }
    }
    LOG(INFO) << "update missing docs stats size:" << items_set.size()
              << " version:" << version;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(MissingDocStatManager);
  IndexBuilder index_builder_;
};

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_MISSING_MANAGER_H_
