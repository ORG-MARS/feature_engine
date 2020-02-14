// Copyright 2019 Netease

// File   doc_stat_index.h
// Author liuzhi
// Date   2019-02-01 15:14:41
// Brief

#ifndef FEATURE_ENGINE_INDEX_DOC_COMMENT_INDEX_TABLE_H_
#define FEATURE_ENGINE_INDEX_DOC_COMMENT_INDEX_TABLE_H_

#include <map>
#include <string>
#include <vector>

#include "feature_engine/index/document.h"
#include "feature_engine/index/index_table_define.h"

namespace feature_engine {

class IndexEntry;

class DocCommentIndexTable {
 public:
  DocCommentIndexTable();
  ~DocCommentIndexTable();

  int Init(IndexEntry* index_entry);

  int Load();
  int Reload() { return Load(); }

  int Update(const std::map<std::string, int32_t>& map_doc_comments_num);
  int UpdateCommentsNum(const std::map<std::string, int32_t>&
                        map_doc_comments_num);

  int GetCommentNum(const std::string& docid, int32_t* comment_num);

  int Refresh();

  inline int size() {
    return doc_comment_table_.size();
  }

 private:
  DocCommentTable doc_comment_table_;
  IndexEntry* index_entry_;
};

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_DOC_COMMENT_INDEX_TABLE_H_
