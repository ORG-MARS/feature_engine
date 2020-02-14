// File   index_entry.h
// Author lidongming
// Date   2018-09-05 13:58:57
// Brief

#ifndef FEATURE_ENGINE_INDEX_INDEX_ENTRY_H_
#define FEATURE_ENGINE_INDEX_INDEX_ENTRY_H_

#include <shared_mutex>

#include <memory>
#include <mutex>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "feature_engine/deps/rapidjson/document.h"
#include "feature_engine/deps/rapidjson/stringbuffer.h"
#include "feature_engine/deps/rapidjson/writer.h"
#include "feature_engine/deps/rapidjson/prettywriter.h"
#include "feature_engine/index/document.h"
#include "feature_engine/index/index_table.h"
#include "feature_engine/index/index_builder.h"
#include "feature_engine/index/doc_comment_index_table.h"
#include "feature_engine/index/index_store.h"
#include "feature_engine/index/doc_consumer.h"
#include "feature_engine/index/doc_stat_index_table.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/commonlib/include/concurrent_queue.h"
#include "feature_engine/deps/boost/include/boost/lockfree/queue.hpp"
#include <random>
#undef signal_set
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio.hpp>

namespace feature_engine {

#define kMaxLockfreeQueueSize 65000
typedef boost::lockfree::queue<std::vector<std::string>*,
          boost::lockfree::capacity<kMaxLockfreeQueueSize>>
            MissingDocLockFreeQueue;

class HttpClient {
using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>
public:
  HttpClient(const std::string &host, const std::string &port, const std::string &target, int version): ioc_(), socket_(ioc_) , host_(host), port_(port), target_(target), version_(version) {};

  ~HttpClient() {
    boost::system::error_code ec;
    (socket_).shutdown(tcp::socket::shutdown_both, ec);
    if (ec) {
      LOG(WARNING) << "[HTTP]socket shutdown err:" << ec.message();
    }
    (socket_).close();
  }

  int connect() {
    try {
      tcp::resolver resolver{ioc_};
      boost::system::error_code ec;
      auto const results = resolver.resolve(host_, port_, ec);
      if (ec) {
        LOG(ERROR) << "[HTTP]resolver resolve err:" << ec.message();
        return -1;
      }
      boost::asio::connect(socket_, results.begin(), results.end(), ec);
      if (ec) {
        LOG(ERROR) << "[HTTP]socket connect err:" << ec.message();
        return -2;
      }
    } catch(std::exception const& e) {
      LOG(WARNING) << "connect Error: " << e.what();
      return -3;
    }
    return 0;
  }

  int getHttpBody(const std::string &rid, const std::vector<std::string> &docids_vec, std::string &json_body) {
    rapidjson::StringBuffer strBuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strBuf);
   
    writer.StartObject();
    writer.Key("item");
    writer.String("ml");
    writer.Key("requestId");
    writer.String(rid.c_str());
    writer.Key("code");
    writer.String("all");
    writer.Key("docid");
    writer.StartArray();
    for (auto docid : docids_vec) {
      writer.String(docid.c_str());
    }
    writer.EndArray();
    writer.EndObject();
    json_body = strBuf.GetString();
    return 0;
  }
  

  void setHttpReq(const std::string &req_json_body) {
    req_.method(boost::beast::http::verb::post);
    req_.version(version_);
    req_.target(target_);
    req_.set(boost::beast::http::field::host, host_);
    req_.set(boost::beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req_.set(boost::beast::http::field::content_type, "application/json");
    req_.body() = req_json_body;
    req_.prepare_payload();  // call prepare_payload before send request
  }

  void sentHttpReq() {
    try {
      boost::beast::http::write(socket_, req_);
    } catch(std::exception const& e) {
      LOG(WARNING) << "sentHttpReq Error: " << e.what();
    }
  }

  boost::beast::http::response<boost::beast::http::dynamic_body> getHttpRes() {
    boost::beast::http::response<boost::beast::http::dynamic_body> res;
    try {
      boost::beast::flat_buffer buffer;
      boost::beast::http::read(socket_, buffer, res);
    } catch(std::exception const& e) {
      LOG(WARNING) << "getHttpRes Error: " << e.what();
    }
    return res;
  }
private:
  boost::asio::io_context ioc_;
  tcp::socket socket_;
  boost::beast::http::request<boost::beast::http::string_body> req_;
  std::string host_;
  std::string port_;
  std::string target_;
  int version_;

};


struct MissingDocIds {
  std::unordered_set<std::string> docids;
  std::mutex* mutex_;
  MissingDocIds() {
    mutex_ = new std::mutex();
  }
  ~MissingDocIds() {
    if (mutex_) {
      delete mutex_;
    }
  }
  void Add(const std::vector<std::string>& ids) {
    std::unique_lock<std::mutex> lock(*mutex_);
    docids.insert(ids.begin(), ids.end());
    // for (const std::string& docid : ids) {
      // docids.insert(docid);
    // }
  }
  void Swap(std::unordered_set<std::string>& ids) {
    std::unique_lock<std::mutex> lock(*mutex_);
    ids.swap(docids);
  }
};

struct MapStringIntAutoLock {
  std::map<std::string, int32_t> doc_comments_num_;
  std::mutex* mutex_;
  MapStringIntAutoLock() {
    mutex_ = new std::mutex();
  }
  ~MapStringIntAutoLock() {
    if (mutex_) {
      delete mutex_;
    }
  }
  void Add(const std::map<std::string, int32_t>& doc_comments_num) {
    std::unique_lock<std::mutex> lock(*mutex_);
    doc_comments_num_.insert(doc_comments_num.begin(), doc_comments_num.end());
  }
  void Swap(std::map<std::string, int32_t>& doc_comments_num) {
    std::unique_lock<std::mutex> lock(*mutex_);
    doc_comments_num_.swap(doc_comments_num);
  }
};


class IndexEntry {
 public:
  IndexEntry();
  ~IndexEntry();

  static IndexEntry& GetInstance() {
    static IndexEntry instance;
    return instance;
  }

  int Init();
  void InitKafkaConsumers();

  int64_t ResetKafkaOffset(int version);

  int Reload();

  void LoadReservedDocs();

  std::shared_ptr<Document> GetDocument(const std::string& docid);
  int GetDocumentsSize();

  void UpdateDocument(const std::shared_ptr<Document>& document) {
    update_document_queue_.Push(document);
  }

  // void AddMissingDoc(const std::vector<std::string>& docids) {
  //   // missing_document_queue_.Push(docids);
  //   std::lock_guard<std::mutex> lock(update_missing_mutex_);
  //   for (const std::string& docid : docids) {
  //     missing_document_set_.insert(docid);
  //   }
  // }

#if 0
  void AddMissingDocs(std::vector<std::string>* docids) {
    if (docids == NULL) {
      return;
    }
    if (!missing_document_queue_.push(docids)) {  // queue is full
      delete docids;
    }
  }

  void AddMissingDocStats(std::vector<std::string>* docids) {
    if (docids == NULL) {
      return;
    }
    if (!missing_doc_stat_queue_.push(docids)) {  // queue is full
      delete docids;
    }
  }
#endif

  void ReloadThread();

  void UpdateThread();
  int BuildIndex(std::vector<std::shared_ptr<Document>>& documents);

  void MonitorThread();
  void MonitorIndex();

  void DumpThread();
  void DumpIndex();

  void UpdateMissingDocumentsThread();

  int GetDocStat(const std::string& docid, DocStat* doc_stat);
  int GetDocComment(const std::string& docid, int32_t* doc_comment);

  MissingDocIds& missing_docs(int seq) {
    return missing_docs_[seq % FLAGS_missing_docids_queue_size];
  }

  MissingDocIds& missing_doc_stats(int seq) {
    return missing_doc_stats_[seq % FLAGS_missing_docids_queue_size];
  }

  MapStringIntAutoLock& updating_doc_comments(int seq) {
    return updating_doc_comments_[
        seq % FLAGS_updating_comment_docids_queue_size];
  }

  st::VersionManager<DocStatIndexTable>& doc_stat_table_vm() {
    return doc_stat_index_table_;
  }
  int ParseHttpRes(const std::vector<std::string> &docids_vec, 
                   const boost::beast::http::response<boost::beast::http::dynamic_body> &res ) ;

 private:
  int Update(std::vector<std::shared_ptr<Document>>& update_batch);
  int Eliminate();

  void UpdateMissingDocStatsThread();
  void UpdateDocCommentsThread();

 private:
  bool builder_;
  bool inited_;

  st::VersionManager<IndexTable> index_table_;
  st::VersionManager<DocStatIndexTable> doc_stat_index_table_;
  st::VersionManager<DocCommentIndexTable> doc_comment_index_table_;  // 评论数

  // SpinMutex update_mutex_;
  std::mutex update_mutex_;
  std::mutex init_mutex_;
  std::mutex doc_stat_mutex_;
  std::mutex doc_comment_mutex_;

  commonlib::ConcurrentQueue<std::shared_ptr<Document>> update_document_queue_;
  // commonlib::ConcurrentQueue<std::string> missing_document_queue_;

#if 0
  MissingDocLockFreeQueue missing_document_queue_;
  MissingDocLockFreeQueue missing_doc_stat_queue_;
#endif

  // std::unordered_set<std::string> missing_document_set_;
  // std::unordered_set<std::string> invalid_document_set_;
  // std::mutex update_missing_mutex_;

  std::vector<std::shared_ptr<DocConsumer>> doc_consumers_;

  IndexBuilder index_builder_;
  IndexStore index_store_;

  std::vector<MissingDocIds> missing_docs_;
  std::vector<MissingDocIds> missing_doc_stats_;
  std::vector<MapStringIntAutoLock> updating_doc_comments_;
};

}   // namespace feature_engine

#endif
