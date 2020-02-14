// Copyright 2019 Netease

// File   doc_stat_index.cpp
// Author lidongming
// Date   2018-12-17 15:35:36
// Brief

#include <unordered_map>
#include <utility>

#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/redis_cluster_client.h"

#include "feature_engine/index/document.h"
#include "feature_engine/index/doc_stat_index_table.h"

namespace feature_engine {

using namespace commonlib;

DocStatIndexTable::DocStatIndexTable() {
}

DocStatIndexTable::~DocStatIndexTable() {
}

int DocStatIndexTable::Init(IndexEntry* index_entry) {
  index_entry_ = index_entry;

  if (doc_stat_table_.init() != 0) {
    return -1;
  }
  return 0;
}

// Load doc stat from redis
int DocStatIndexTable::Load() {
  return 0;
}

int DocStatIndexTable::GetDocStat(const std::string& docid, DocStat* doc_stat) {
  auto it = doc_stat_table_.seek<DOCID>(docid);
  if (it != (long)NULL) {
    *doc_stat = it->at<DOC_STAT>();
    return 0;
  }
  return -1;
}

int DocStatIndexTable::Update(const std::vector<std::string>& docids) {
  if (docids.empty()) {
    return 0;
  }
  return UpdateDocStats(docids);
}

int DocStatIndexTable::Refresh() {
  DocStatTable::Iterator it =  doc_stat_table_.begin();
  int64_t current_time = TimeUtils::GetCurrentTime();
  std::vector<std::string> erase_docs;
  int count = 0;
  for (; it != doc_stat_table_.end(); ++it) {
    if (++count > FLAGS_max_doc_stat_count) {
      erase_docs.emplace_back(it->at<DOCID>());
      continue;
    }
    const DocStat& doc_stat = it->at<DOC_STAT>();
    if (doc_stat.IsExpired(current_time)) {
      erase_docs.emplace_back(doc_stat.docid);
    }
  }

  if (!erase_docs.empty()) {
    for (const std::string& docid : erase_docs) {
      doc_stat_table_.erase<DOCID>(docid);
    }
  }
  // LOG(INFO) << "refresh doc_stat table erase_docs_count:" << erase_docs.size();
  return 0;
}

int DocStatIndexTable::UpdateDocStats(const std::vector<std::string>& docids) {
  RedisClusterClient redis_cluster_client(FLAGS_doc_stat_redis_host, 0);
  int64_t current_time = TimeUtils::GetCurrentTime();
  const static std::string doc_stat_prefix = "DSD_";
  std::vector<std::string> sub_docids;
  int docids_last_idx = docids.size() - 1;
  for (int i = 0; i < docids.size(); i++) {
    if (sub_docids.size() < FLAGS_max_mget_count) {
      sub_docids.emplace_back(docids[i]);
      // 不是本批 mget 最后一个，也不是全局最后一个时，保存 docid 后跳过
      if (sub_docids.size() != FLAGS_max_mget_count && i != docids_last_idx) {
        continue;
      }
    }
    std::unordered_map<std::string, std::string> res;
    if (redis_cluster_client.mget(doc_stat_prefix, sub_docids, res) != 0) {
      LOG(WARNING) << "mget doc stat from redis error";
      continue;
    }

    for (const auto& kv : res) {
      DocStat doc_stat;
      if (doc_stat.Parse(kv.second) == 0) {
        doc_stat.set_valid(true);
        doc_stat.timestamp = current_time;
        doc_stat_table_.insert(kv.first, std::move(doc_stat));
      }
    }
    sub_docids.clear();
  }
  return 0;
}

}   // namespace feature_engine
