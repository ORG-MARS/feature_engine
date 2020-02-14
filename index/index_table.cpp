// File   index_table.cpp
// Author lidongming
// Date   2018-09-05 14:11:39
// Brief

#include "feature_engine/index/index_table.h"
#include <stdio.h>
#include <vector>
#include <fstream>
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/commonlib/include/file_utils.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/string_utils.h"
#include "feature_engine/deps/commonlib/include/thread_pool.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/index/document.h"
#include "feature_engine/index/index_entry.h"

namespace feature_engine {

using namespace commonlib;

//用来读index文件的线程屏障
pthread_barrier_t load_indexfile_barrier;

IndexTable::IndexTable() { }

IndexTable::~IndexTable() { }

int IndexTable::Init(IndexEntry* index_entry) {
  index_entry_ = index_entry;
  return InitTables();
}

int IndexTable::InitTables() {
  if (docinfo_table_.init() != 0) {
    return -1;
  }
  return 0;
}

// Macros
#define CHECK_INDEX_FILE(_index_path_, _index_file_name_)                      \
  do {                                                                         \
    std::string index_file = _index_path_ + "/" + _index_file_name_;           \
    if (!FileUtils::FileExists(index_file)) {                                  \
      LOG(WARNING) << "index file not exist:" << index_file;                   \
      return -1;                                                               \
    }                                                                          \
  } while(0)

#define LOAD_INDEX_FILE(_table_name_, _index_table_, _index_path_,             \
    _index_file_name_)                                   \
do {                                                                           \
  st::MetaData meta_data;                                                      \
  meta_data.version = 0;                                                       \
  meta_data.partition = -1;                                                    \
  strcpy(meta_data.name, _table_name_);                                        \
  std::string index_file_name = _index_file_name_; \
  _index_table_.clear();                                                       \
  int retval = _index_table_.load(_index_path_.c_str(),                        \
      index_file_name.c_str(), meta_data);     \
  if (retval != 0) {                                                           \
    LOG(WARNING) << "load index error path:" << _index_path_                   \
    <<  " file:" << index_file_name.c_str();                \
    return -1;                                                                 \
  }                                                                            \
  LOG(INFO) << "load index succesfully path:" << _index_path_                  \
  << " file:" << _index_file_name_                               \
  << " size:" << _index_table_.size();                           \
} while(0)

int IndexTable::LoadDocinfoFile(const std::string& docinfo_index_file,
                int file_num, std::vector<std::shared_ptr<Document>>* docs) {
  int pos = file_num * FLAGS_dump_count_per_file;
  std::ifstream ifs(docinfo_index_file);
  if (!ifs.is_open()) {
    LOG(ERROR) << "read docinfo file error:" << docinfo_index_file;
    pthread_barrier_wait(&load_indexfile_barrier);
    return -1;
  }
  std::string docinfo_str;
  docinfo_str.assign(std::istreambuf_iterator<char>(ifs),
      std::istreambuf_iterator<char>());
  ifs.close();
  if (docinfo_str.size() == 0) {
    pthread_barrier_wait(&load_indexfile_barrier);
    return 0;
  }

  feature_proto::FeaturesList feature_list;
  if (!feature_list.ParseFromString(docinfo_str)) {
    LOG(WARNING) << "parse features file " << docinfo_index_file << " failed";
    pthread_barrier_wait(&load_indexfile_barrier);
    return -1;
  }
  int features_size = feature_list.features_size();
  LOG(INFO) << "docfile:" << docinfo_index_file << " ,pos is:" << pos
            << " ,features_size:" << features_size;
  if (features_size == 0) {
    LOG(WARNING) << "features file " << docinfo_index_file
                 << " no valid features";
    pthread_barrier_wait(&load_indexfile_barrier);
    return -1;
  }
  for (int i = 0; i < features_size; ++i, ++pos) {
    (*docs)[pos] = nullptr;
    feature_proto::FeaturesMap& feature_map =
      const_cast<feature_proto::FeaturesMap&>(feature_list.features(i));
    std::shared_ptr<Document> document = std::make_shared<Document>();
    document->set_feature_map(std::move(feature_map));
    document->Init();
  
    Status status = document->IsValid();
    if (document->docid().empty() || !status.ok()) {
      continue;
    }
    DLOG(INFO) << "document docid:" << document->docid();
    (*docs)[pos] = std::move(document);
  }
  pthread_barrier_wait(&load_indexfile_barrier);
  return 0;
}

// Load indexes from local files
int IndexTable::Load() {
  LOG(INFO) << "start load index from local";
  int retval = 0;

  std::string load_index_path = FLAGS_load_index_path;

  // Check docinfo forward index
//  CHECK_INDEX_FILE(FLAGS_load_index_path, FLAGS_docinfo_file);

#if 0
  std::string docinfo_index_file = load_index_path + "/" + FLAGS_docinfo_file;
  if (!FileUtils::FileExists(docinfo_index_file)) {
    LOG(WARNING) << "docinfo index file not exist:" << docinfo_index_file;
    return -1;
  }
#endif

  // Load forward index
  std::vector<std::string> docinfo_files = FileUtils::ListDir(load_index_path);
  if (docinfo_files.empty()) {
    LOG(ERROR) << "docinfo_files in index is empty";
    return -1;
  }
  DLOG(INFO) << "load index path file size is:" << docinfo_files.size();
  commonlib::ThreadPool index_threadpool(docinfo_files.size(), 
      commonlib::AFFINITY_DISABLE, 0);
  pthread_barrier_init(&load_indexfile_barrier, NULL, docinfo_files.size() + 1);
  docinfo_table_.clear();

  std::vector<std::shared_ptr<Document>> docs;
  docs.resize(docinfo_files.size() * FLAGS_dump_count_per_file);
  for (int i = 0; i < docinfo_files.size(); ++i) {
    index_threadpool.enqueue( std::bind(&IndexTable::LoadDocinfoFile, 
            this, docinfo_files[i], i, &docs));
  }
  pthread_barrier_wait(&load_indexfile_barrier);
  Update(docs);
  LOG(INFO) << "load docinfo table size:" << docinfo_table_.size();

  return 0;
}

std::shared_ptr<Document> IndexTable::GetDocument(const std::string& docid) {
  auto it = docinfo_table_.seek<DOCID>(docid);
  if (it != (long)NULL) {
    return it->at<DOCINFO>();
  }
  return NULL;
}

// WARNING:Not thread safe
int IndexTable::Update(std::vector<std::shared_ptr<Document>>& update_batch) {
  for (auto& doc : update_batch) {
    if (doc == nullptr) {
      continue;
    }
    Status valid_status = doc->IsValid();
    if (!valid_status.ok()) {
      continue;
    }

    std::string docid = doc->docid();

    // Update docinfo forward index table
    docinfo_table_.insert(docid, std::move(doc));
    if (FLAGS_debug) {
      LOG(INFO) << "insert doc:" << docid;
    }
  }
  return 0;
}

int IndexTable::Eliminate() {
  // Remove expire docs
  DocInfoTable::Iterator it = docinfo_table_.begin();
  // int64_t current_time = TimeUtils::GetCurrentTime();
  std::vector<std::string> expire_docs;
  for (; it != docinfo_table_.end(); ++it) {
    auto& document = it->at<DOCINFO>();
    // if (document->expire_time() <= current_time) {
    Status status = document->IsValid();
    if (!status.ok()) {
      expire_docs.emplace_back(it->at<DOCID>());
    }
  }
#if 0
  for (auto& docid : expire_docs) {
    docinfo_table_.erase<DOCID>(docid);
    LOG(INFO) << "erase doc:" << docid;
  }
#endif
  LOG(INFO) << "remove expire docs size:" << expire_docs.size();

  // Remove extra docs
  int diff = GetDocumentsSize() - FLAGS_doc_limit;
#if 0
  int limit_count = FLAGS_doc_limit * FLAGS_load_factor;

  if (diff > limit_count) {
    std::vector<std::string> removed_docs;
    int count = 0;
    DocInfoTable::Iterator it = docinfo_table_.begin();
    for (; it != docinfo_table_.end(); ++it) {
      if (++count >= limit_count) {
        removed_docs.push_back(it->at<DOCID>());
      }
    }
    for (auto& docid : removed_docs) {
      docinfo_table_.erase<DOCID>(docid);
    }
    LOG(INFO) << "remove extra docs size:" << removed_docs.size();
  }
#endif

  if (diff > 0) {
    std::vector<std::string> removed_docs;
    int count = 0;
    DocInfoTable::Iterator it = docinfo_table_.begin();
    for (; it != docinfo_table_.end(); ++it, ++count) {
      if (count > diff) {
        break;
      } else {
        removed_docs.emplace_back(it->at<DOCID>());
      }
    }
    for (const std::string& docid : removed_docs) {
      docinfo_table_.erase<DOCID>(docid);
    }
    // LOG(INFO) << "remove extra docs size:" << removed_docs.size();
    LOG(INFO) << "docs size:" << docinfo_table_.size() << " removed:" << diff;
  }
  return 0;
}

void IndexTable::MonitorIndex() {
  LOG(INFO) << "docinfo_index_size:" << docinfo_table_.size()
    << " docinfo_table_mem:" << docinfo_table_.mem();
}

// Clear old indexes
void IndexTable::ClearIndex() {
  docinfo_table_.clear();
}

int IndexTable::WriteIntoFile(const std::string &docinfo_index_file_new, const feature_proto::FeaturesList &feature_list) {
  std::string docinfo_index_file_bak = docinfo_index_file_new + ".bak";
  std::string serialize_str;
  std::ofstream ofs(docinfo_index_file_bak);
  if (!ofs.is_open()) {
    LOG(WARNING) << "open docinfo index bak file error:"
      << docinfo_index_file_bak;
    return -1;
  }
  feature_list.SerializeToString(&serialize_str);
  LOG(INFO) << "docinfo_file is:" << docinfo_index_file_new << ",dump feature size:" << feature_list.features_size();
  ofs << serialize_str;
  ofs.close();
  // Rename new index files
  if (rename(docinfo_index_file_bak.c_str(), docinfo_index_file_new.c_str())) {
    LOG(WARNING) << "rename docinfo index file error:" << docinfo_index_file_new;
    return -1;
  }
  return 0;
}


  // Dump indexes to local file
void IndexTable::DumpIndex() {
  std::string docinfo_index_file = FLAGS_dump_index_path + "/" + FLAGS_docinfo_file;
  std::vector<std::string> docinfo_files_old = FileUtils::ListDir(FLAGS_dump_index_path);
  std::vector<std::string> docinfo_files_new;
  std::string serialize_str;
  feature_proto::FeaturesList feature_list;
  DocInfoTable::Iterator it = docinfo_table_.begin();
  int docinfo_count = 1;
  // The number of new generated files, meanwhile as postfix of new file
  int file_num = 0;
  for (; it != docinfo_table_.end(); ++it, ++docinfo_count) {
    std::shared_ptr<Document>& document = it->at<DOCINFO>();
    if (document != nullptr) {
      feature_proto::FeaturesMap& feature_map = *document->feature_map();
      auto f = feature_list.add_features();
      *f = feature_map;
    }
    if (!(docinfo_count % FLAGS_dump_count_per_file)) {
      std::string docinfo_index_file_new = docinfo_index_file + std::to_string(file_num);
//      LOG(INFO) << "feature_list size:" << feature_list.features_size();
      int ret = WriteIntoFile(docinfo_index_file_new, feature_list);
      if (ret != 0) {
        LOG(WARNING) << "writeintofile " << docinfo_index_file_new << " error";
        continue;
      }
      docinfo_files_new.push_back(docinfo_index_file_new);
      feature_list.Clear();
      ++file_num;
    }
  }
  //最后剩余的零散文章，写进最后一个文件
  std::string docinfo_index_file_new = docinfo_index_file + std::to_string(file_num);
//  LOG(INFO) << "feature_list size:" << feature_list.features_size();
  int ret = WriteIntoFile(docinfo_index_file_new, feature_list);
  if (ret != 0) {
    LOG(WARNING) << "writeintofile " << docinfo_index_file_new << " error";
  }
  docinfo_files_new.push_back(docinfo_index_file_new);
  //为了防止新生成的文件数量比原来的少，多出来老文件不会被覆盖的情况，所有新文件dump完毕将多余的老文件删除
  for (auto file : docinfo_files_old) {
    std::vector<std::string>::iterator it = find(docinfo_files_new.begin(), docinfo_files_new.end(), file);
    if (it == docinfo_files_new.end() && remove(file.c_str()) != 0) {
        LOG(WARNING) << "remove docinfo file error:" << docinfo_index_file_new;
    }
  }
  LOG(INFO) << "finish dump docinfo_index";
}

}//end of namespace 
