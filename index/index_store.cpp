
#include <fstream>
#include "feature_engine/index/index_store.h"
#include "feature_engine/index/redis_adapter.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/boost/include/boost/algorithm/string/join.hpp"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/thread_pool.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/commonlib/include/file_utils.h"
#include <boost/algorithm/string.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio.hpp>

namespace feature_engine {

using namespace commonlib;
pthread_barrier_t load_jsonfile_barrier;

IndexStore::IndexStore() { }

IndexStore::~IndexStore() { }

int IndexStore::Init() { return 0; }

int IndexStore::GetAllDocsFromMysql(std::vector<std::shared_ptr<Document>>* docs) {
  int64_t total_num = FLAGS_common_docs_limit + FLAGS_video_docs_limit + FLAGS_short_video_docs_limit;
  docs->resize(total_num);
  LoadjsonFromFileParallel(docs);
  return 0;
}

int IndexStore::LoadjsonFromFileParallel(std::vector<std::shared_ptr<Document>>* docs) {
  std::string load_index_path = FLAGS_load_index_path;
  std::vector<std::string> docinfo_files = FileUtils::ListDir(load_index_path);
  if (docinfo_files.empty()) {
    LOG(ERROR) << "docinfo_files in <" << load_index_path  << "> is empty";
    return -1;
  }
  LOG(INFO) << "load index path:" << load_index_path << ",file num is:" << docinfo_files.size();
  commonlib::ThreadPool index_threadpool(docinfo_files.size(), 
      commonlib::AFFINITY_DISABLE, 0);
  pthread_barrier_init(&load_jsonfile_barrier, NULL, docinfo_files.size() + 1);
  for (int i = 0; i < docinfo_files.size(); ++i) {
    index_threadpool.enqueue( std::bind(&IndexStore::LoadJsonFromFile, 
            this, docinfo_files[i], i, docs));
  }
  pthread_barrier_wait(&load_jsonfile_barrier);
  return 0;
}

int IndexStore::LoadJsonFromFile(const std::string& docinfo_index_file,
                int file_num, std::vector<std::shared_ptr<Document>>* docs) {
  // FLAGS_dump_count_per_file 需要和download脚本中的每个文件的条数保持一致
  int pos = file_num * FLAGS_dump_count_per_file;
  std::ifstream ifs(docinfo_index_file);
  if (!ifs.is_open()) {
    LOG(ERROR) << "read docinfo file error:" << docinfo_index_file;
    pthread_barrier_wait(&load_jsonfile_barrier);
    return -1;
  }
  std::string json_value;
  int parse_success_count = 0;
  int parse_fail_count = 0;
  while (getline(ifs, json_value)) {
    (*docs)[pos] = nullptr;
    if (json_value.empty()) {
      // LOG(WARNING) << "json value is empty docid:" << docid;
      parse_fail_count++;
    } else {
      std::shared_ptr<Document> document = std::make_shared<Document>();
      Status status = document->ParseFromJson(json_value);
      if (status.ok()) {
        (*docs)[pos] = std::move(document);
        parse_success_count++;
      } else {
        parse_fail_count++;
        LOG(WARNING) << "parse document from json value error json_value:" << json_value;
      }
      pos++;
    }
  }
  ifs.close();
  LOG(INFO) << "parse document success count:" << parse_success_count << ", parse document fail count:" << parse_fail_count;
  pthread_barrier_wait(&load_jsonfile_barrier);
  return 0;
}

// 文章类型
// | video        |
// | doc          |
// | photoset     |
// | special      |
// | live         |
// | luobo        |
// | videospecial |
// | mint         |
// | shortvideo   |
// | wcteam       |
// | opencourse   |
// | motif        |
// | hamletmotif  |
// | videoalbum   |
// | rec          |          



//
std::shared_ptr<MysqlConf> IndexStore::MakeMysqlConf() {
  return std::make_shared<MysqlConf>(
      FLAGS_mysql_port, FLAGS_mysql_host,
      FLAGS_mysql_user, FLAGS_mysql_passwd,
      FLAGS_mysql_database);
}


std::shared_ptr<Document> IndexStore::GetDocumentFromJson(const std::string& json_value) {
  std::shared_ptr<Document> document = std::make_shared<Document>();
  Status status = document->ParseFromJson(json_value);
  if (status.ok()) {
    // doc->reset(document);
    // *doc = std::move(document);
    return document;
  } else {
    LOG(WARNING) << "parse document from json value error";
    return nullptr;
  }
  return nullptr;
}

}
