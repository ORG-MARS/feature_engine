// Copyright 2019 Netease

// File   doc_stat_index.h
// Author lidongming
// Date   2018-12-17 15:14:41
// Brief

#ifndef FEATURE_ENGINE_INDEX_DOC_STAT_INDEX_TABLE_H_
#define FEATURE_ENGINE_INDEX_DOC_STAT_INDEX_TABLE_H_

#include <string>
#include <vector>

#include "feature_engine/index/document.h"
#include "feature_engine/index/index_table_define.h"

namespace feature_engine {

class IndexEntry;

class DocStatIndexTable {
 public:
  DocStatIndexTable();
  ~DocStatIndexTable();

  int Init(IndexEntry* index_entry);

  int Load();
  int Reload() { return Load(); }

  int Update(const std::vector<std::string>& docids);
  int UpdateDocStats(const std::vector<std::string>& docids);

  int GetDocStat(const std::string& docid, DocStat* doc_stat);

  int Refresh();

  inline int size() {
    return doc_stat_table_.size();
  }

 private:
  DocStatTable doc_stat_table_;  // day
//   DocStatTable doc_stat_table_week;
//   DocStatTable doc_stat_table_hour;
  IndexEntry* index_entry_;
};

}   // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_DOC_STAT_INDEX_TABLE_H_
