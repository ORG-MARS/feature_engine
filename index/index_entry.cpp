// File   index_entry.cpp
// Author lidongming
// Date   2018-09-05 17:07:05
// Brief

#include <mutex>
#include <thread>
#include <fstream>
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

#include "feature_engine/index/index_entry.h"
#include "feature_engine/index/doc_consumer.h"
#include "feature_engine/deps/commonlib/include/monitor.h"
#include "feature_engine/deps/commonlib/include/file_utils.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/redis_cluster_client.h"
#include <cstdlib>
#include <iostream>
#include <string>

namespace feature_engine {

using namespace commonlib;

IndexEntry::IndexEntry() : inited_(false) { }

IndexEntry::~IndexEntry() {  }

// WARNING:should be inited before using
int IndexEntry::Init() {
    std::unique_lock<std::mutex> lock(init_mutex_);

    if (inited_) return 0;

    missing_docs_.resize(FLAGS_missing_docids_queue_size);
    missing_doc_stats_.resize(FLAGS_missing_docids_queue_size);
    updating_doc_comments_.resize(FLAGS_updating_comment_docids_queue_size);

    // 1.Init smalltable at first---------------------------
    // Init version_manager of index table
    if (index_table_.init(2, "index_table") != 0) { return -1; }

    // Init index smalltable
    int version = index_table_.create_version();
    if (version < 0) { return -1; }
    if (index_table_[version].Init(this) != 0) {
      LOG(WARNING) << "init index table error";
        index_table_.drop_version(version);
        return -1;
    }

    if (doc_stat_index_table_.init(2, "doc_stat_index_table") != 0) { return -1; }
    int doc_stat_version = doc_stat_index_table_.create_version();
    if (doc_stat_version < 0) { return -1; }
    if (doc_stat_index_table_[doc_stat_version].Init(this) != 0) {
      LOG(WARNING) << "init index table error";
        doc_stat_index_table_.drop_version(doc_stat_version);
        return -1;
    }

    if (doc_comment_index_table_.init(2, "doc_comment_index_table") != 0) { return -1; }
    int doc_comment_version = doc_comment_index_table_.create_version();
    if (doc_comment_version < 0) { return -1; }
    if (doc_comment_index_table_[doc_comment_version].Init(this) != 0) {
      LOG(WARNING) << "init doc_comment_index_table_ error";
      LOG(WARNING) << "init index table error";
      doc_comment_index_table_.drop_version(doc_comment_version);
      return -1;
    }


    // 2.Load old indexes from local file--------------------
    // Init first version and load index tables
    if (!FLAGS_rebuild_total_index) {
      if (index_table_[version].Load() != 0) {
        LOG(WARNING) << "load index table error";
      } else {
        LOG(INFO) << "load index table successfully";
      }
    }

    // Freeze table
    if (index_table_.freeze_version(version) != 0) {
      index_table_.drop_version(version);
      return -1;
    }

    inited_ = true;

    //// 3.Load docs from mysql -----------------------------
    std::vector<std::shared_ptr<Document>> docs;
    index_store_.GetAllDocsFromMysql(&docs);

    // Update documents
    Update(docs);


    // 4.Consumedocs from kafka------------------------------
    // Init Kafka Consumers
    if (FLAGS_enable_kafka) {
      InitKafkaConsumers();
    }

    if (FLAGS_load_reserved_docs) {
      LoadReservedDocs();
    }


    // Start detached threads
    std::thread(&IndexEntry::UpdateThread, this).detach();
    std::thread(&IndexEntry::MonitorThread, this).detach();
//    std::thread(&IndexEntry::DumpThread, this).detach();
    // std::thread(&IndexEntry::UpdateMissingDocumentsThread, this).detach();
    // std::thread(&IndexEntry::UpdateMissingDocStatsThread, this).detach();
    std::thread(&IndexEntry::UpdateDocCommentsThread, this).detach();

    return 0;
}

void IndexEntry::LoadReservedDocs() {
  std::ifstream ifs(FLAGS_reserved_docs);
  if (!ifs.is_open()) {
    LOG(WARNING) << "open reserved docs error:" << FLAGS_reserved_docs;
    return;
  }
  std::string line;
  std::vector<std::string> docids;
  while (std::getline(ifs, line)) {
	  boost::trim(line);
    docids.emplace_back(line);
  }
  ifs.close();

  if (docids.empty()) {
    return;
  }

  std::vector<std::shared_ptr<Document>> docs;
  docs.resize(docids.size());
  index_builder_.GetDocumentsFromRedisParallel(docids, &docs);

  Update(docs);
  LOG(INFO) << "load reserved docs count:" << docs.size();
}

void IndexEntry::InitKafkaConsumers() {
    // Start consumers
	LOG(INFO) << "start get kafka info";
    for (int i = 0; i < FLAGS_partition_count; i++) {
        std::shared_ptr<DocConsumer> consumer
          = std::make_shared<DocConsumer>(i, this);
        consumer->Start();
        doc_consumers_.push_back(consumer);
    }
}

// Update documents(from kafka) into indexes
int IndexEntry::Update(std::vector<std::shared_ptr<Document>>& update_batch) {
  if (update_batch.empty()) {
    return 0;
  }

  std::lock_guard<std::mutex> lock(update_mutex_);
  int version = index_table_.create_version();
  if (version < 0) { return -1; }
  if (index_table_[version].Update(update_batch) != 0) {
    index_table_.drop_version(version);
    return -1;
  }

  // Eliminate extra docs
  // index_table_[version].Eliminate();

  if (index_table_.freeze_version(version) != 0) {
    index_table_.drop_version(version);
    return -1;
  }

  return 0;
}

// Eliminate documents
int IndexEntry::Eliminate() {
  std::lock_guard<std::mutex> lock(update_mutex_);
  int version = index_table_.create_version();
  if (version < 0) {
    return -1;
  }
  if (index_table_[version].Eliminate() != 0) {
    index_table_.drop_version(version);
    LOG(WARNING) << "eliminate index error version:" << version;
    return -1;
  }

  if (index_table_.freeze_version(version) != 0) {
    index_table_.drop_version(version);
    return -1;
  }

  return 0;
}

// Update thread
void IndexEntry::UpdateThread() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  long last_update_time = tv.tv_sec;
  long last_eliminate_time = tv.tv_sec;

  std::vector<std::shared_ptr<Document>> update_batch;
  while (1) {
    // LOG(INFO) << "update document queue size:" << update_document_queue_.Size();
    if (update_document_queue_.Empty()) {
      usleep(1000 * 100);
    }

    gettimeofday(&tv, NULL);
    long cur_ts = tv.tv_sec;

    if (cur_ts - last_eliminate_time >= FLAGS_eliminate_index_interval) {
      Eliminate();
      last_eliminate_time = cur_ts;
    }

    std::shared_ptr<Document> document;
    if (update_document_queue_.TryPop(document)) {
      update_batch.push_back(document);
    }
    if (cur_ts - last_update_time >= FLAGS_update_index_interval) {
      //LOG(INFO) << "start update batch:" << update_batch.size();
      Update(update_batch);
      update_batch.clear();
      last_update_time = cur_ts;
    }
  }
}

void IndexEntry::DumpThread() {
  while(1) {
    sleep(FLAGS_dump_index_interval);
    DumpIndex();
  }
}

// Dump index to local file
void IndexEntry::DumpIndex() {
  std::lock_guard<std::mutex> lock(update_mutex_);

  int pos = index_table_.find_latest_read_only();
  if (pos < 0) { return; }
  index_table_[pos].DumpIndex();
}

// Monitor and Log size of each index table
void IndexEntry::MonitorThread() {
  while (1) {
    sleep(FLAGS_monitor_index_interval);
    MonitorIndex();
  }
}

// Monitor thread
void IndexEntry::MonitorIndex() {
  std::lock_guard<std::mutex> lock(update_mutex_);
  int version = index_table_.find_latest_read_only();
  if (version < 0) { return; }
  index_table_[version].MonitorIndex();
}

int IndexEntry::ParseHttpRes(const std::vector<std::string> &docids_vec, const boost::beast::http::response<boost::beast::http::dynamic_body> &res ) {
  namespace http = boost::beast::http;    // from <boost/beast/http.hpp>
  DLOG(INFO) << "http res body is: [" << buffers_to_string(res.body().data()) << "]";
  rapidjson::Document res_json;
  rapidjson::ParseResult ok = res_json.Parse(buffers_to_string(res.body().data()).c_str());
  if (!ok) {
    LOG(INFO) << "PARSE json err: " << ok.Code() << ", " << ok.Offset();
    return -1;
  }
  if (!res_json.IsObject() || !res_json.HasMember("data")) { 
    LOG(WARNING) << "res_json has no data";
    return -1;
  }
  const rapidjson::Value &data_object = res_json["data"];
  if(!data_object.IsObject()) {
    return -1;
  }
  for (auto docid : docids_vec) {
    if(!data_object.HasMember(docid.c_str()) || !data_object[docid.c_str()].IsObject()) {
      continue;
    }
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer( sb );
    data_object[docid.c_str()].Accept( writer );
    DLOG(INFO) << "docid:" << docid << ", json_value is:" << sb.GetString();

    std::shared_ptr<Document> document = std::move(
        index_store_.GetDocumentFromJson(sb.GetString()));
    if (document != nullptr) {
      Status valid_status = document->IsValid();
      if (valid_status.ok()) {
        UpdateDocument(document);
        DLOG(INFO) << "update missing document docid:" << docid;
      } else {
        DLOG(INFO) << "expire document docid:" << docid;
      }
    } else {
      LOG(INFO) << "invalid document docid:" << docid;
    }
  }
  return 0;
}

void IndexEntry::UpdateMissingDocumentsThread() {
  using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>
  namespace http = boost::beast::http;    // from <boost/beast/http.hpp>

  std::string const host = FLAGS_http_server_host;
  std::string const port = FLAGS_http_server_port;
  std::string const target = FLAGS_http_server_target;
  int version = 11;

  std::unordered_set<std::string> invalid_docids_set;
  int64_t last_update_time = TimeUtils::GetCurrentTime();

//while
  while (true) {
    int64_t current_time = TimeUtils::GetCurrentTime();
    if (current_time - last_update_time > FLAGS_update_missing_interval * 1000) {
      std::unordered_set<std::string> missing_docids_set;
      for (MissingDocIds& missing_docids : missing_docs_) {
        std::unordered_set<std::string> docids_set;
        missing_docids.Swap(docids_set);
        missing_docids_set.insert(docids_set.begin(), docids_set.end());
      }
      LOG(INFO) << "start update missing docs size:"
                << missing_docids_set.size()
                << " invalid docs size:" << invalid_docids_set.size();

      int batch = 0;
      int doc_count = 0;
      int total_count = 1;
      std::vector<std::string> docids_vec;

      int missing_doc_size = missing_docids_set.size();
      HttpClient http_client(host, port, target, version);
      int connect_ret = http_client.connect();
      if (connect_ret == 0) {
        for (auto docid : missing_docids_set) {
          if (docid.empty()) {
            continue;
          }
          if (FLAGS_skip_invalid_doc) {
            if (invalid_docids_set.find(docid) != invalid_docids_set.end()) {
              continue;
            }
          }
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

            int parse_res = ParseHttpRes(docids_vec,res);
            docids_vec.clear();
            doc_count = 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          } 
          total_count++;
        }
        last_update_time = current_time;
        LOG(INFO) << "finish update missing docs size:"
                  << missing_docids_set.size()
                  << " invalid docs size:" << invalid_docids_set.size();
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  } // while done
}


int IndexEntry::GetDocumentsSize() {
  int pos = index_table_.find_latest_read_only();
  if (pos < 0) { return -1; }
  return index_table_[pos].GetDocumentsSize();
}

std::shared_ptr<Document> IndexEntry::GetDocument(const std::string& docid) {
  int pos = index_table_.find_latest_read_only();
  if (pos < 0) { return nullptr; }
  return index_table_[pos].GetDocument(docid);
}

int IndexEntry::GetDocStat(const std::string& docid, DocStat* doc_stat) {
  int pos = doc_stat_index_table_.find_latest_read_only();
  if (pos < 0) { return -1; }
  return doc_stat_index_table_[pos].GetDocStat(docid, doc_stat);
}

int IndexEntry::GetDocComment(const std::string& docid, int32_t* doc_comment) {
  int pos = doc_comment_index_table_.find_latest_read_only();
  if (pos < 0) { return -1; }
  return doc_comment_index_table_[pos].GetCommentNum(docid, doc_comment);
}

void IndexEntry::UpdateMissingDocStatsThread() {
  std::string docid;
  int64_t last_update_time = TimeUtils::GetCurrentTime();
  while (true) {
    int64_t current_time = TimeUtils::GetCurrentTime();

    if (current_time - last_update_time > FLAGS_update_missing_interval * 1000) {
      std::unordered_set<std::string> missing_doc_stats_set;
      for (MissingDocIds& missing_docids : missing_doc_stats_) {
        std::unordered_set<std::string> docids_set;
        missing_docids.Swap(docids_set);
        missing_doc_stats_set.insert(docids_set.begin(), docids_set.end());
      }
 
      std::vector<std::string> docids;
      for (const std::string& docid : missing_doc_stats_set) {
        docids.emplace_back(docid);
      }

      LOG(INFO) << "start update missing doc stats size:" << docids.size();
      if (docids.size() > 0) {
        std::lock_guard<std::mutex> lock(doc_stat_mutex_);
        int version = doc_stat_index_table_.create_version();
        if (version >= 0) {
          doc_stat_index_table_[version].Refresh();
          if (doc_stat_index_table_[version].Update(docids) != 0) {
            doc_stat_index_table_.drop_version(version);
          } else {
            if (doc_stat_index_table_.freeze_version(version) != 0) {
              doc_stat_index_table_.drop_version(version);
            } else {
              LOG(INFO) << "doc stats size:" << doc_stat_index_table_[version].size();
            }
          }
        }
      }  // End lock
      LOG(INFO) << "finish update missing doc stats size:" << docids.size();

      last_update_time = current_time;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void IndexEntry::UpdateDocCommentsThread() {
  std::string docid;
  int64_t last_update_time = TimeUtils::GetCurrentTime();
  while (true) {
    int64_t current_time = TimeUtils::GetCurrentTime();

    if (current_time - last_update_time
        < FLAGS_update_comment_interval * 1000) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    std::map<std::string, int32_t> map_doc_comments_num;
    for (MapStringIntAutoLock& item : updating_doc_comments_) {
      std::map<std::string, int32_t> sub_map;
      item.Swap(sub_map);
      map_doc_comments_num.insert(sub_map.begin(), sub_map.end());
    }

    LOG(INFO) << "start update comments num size:"
        << map_doc_comments_num.size();
    if (map_doc_comments_num.size() > 0) {
      std::lock_guard<std::mutex> lock(doc_comment_mutex_);
      int version = doc_comment_index_table_.create_version();
      if (version >= 0) {
        doc_comment_index_table_[version].Refresh();
        if (doc_comment_index_table_[version].Update(map_doc_comments_num)
            != 0) {
          doc_comment_index_table_.drop_version(version);
        } else {
          if (doc_comment_index_table_.freeze_version(version) != 0) {
            doc_comment_index_table_.drop_version(version);
          } else {
            LOG(INFO) << "doc_comment_index_table_ size:"
                << doc_comment_index_table_[version].size();
          }
        }
      }
    }  // End lock
    LOG(INFO) << "finish update comments num size:"
        << map_doc_comments_num.size();

    last_update_time = current_time;
  }
}

}  // namespace feature_engine
