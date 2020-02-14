// Copyright 2019 Netease

// File   doc_comment_index_table.cpp
// Author liuzhi
// Date   2019-02-21 17:57
// Brief

#include <memory>

#include "feature_engine/index/doc_comment_index_table.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/redis_cluster_client.h"

namespace feature_engine {

using namespace commonlib;

DocCommentIndexTable::DocCommentIndexTable() {
}

DocCommentIndexTable::~DocCommentIndexTable() {
}

int DocCommentIndexTable::Init(IndexEntry* index_entry) {
  index_entry_ = index_entry;

  if (doc_comment_table_.init() != 0) {
    return -1;
  }
  return 0;
}

// Load doc comment from redis
int DocCommentIndexTable::Load() {
  return 0;
}

int DocCommentIndexTable::GetCommentNum(const std::string& docid,
                                        int32_t* comment_num) {
  auto it = doc_comment_table_.seek<DOCID>(docid);
  if (it != (long)NULL) {
    const std::shared_ptr<DocComment>& doc_comment = it->at<DOC_COMMENT>();
    *comment_num = doc_comment->comment_num;
    return 0;
  }
  return -1;
}

int DocCommentIndexTable::Refresh() {
  DocCommentTable::Iterator it =  doc_comment_table_.begin();
  int64_t current_time = TimeUtils::GetCurrentTime();
  std::vector<std::string> erase_docs;
  int count = 0;
  for (; it != doc_comment_table_.end(); ++it) {
    if (++count > FLAGS_max_doc_comment_count) {
      erase_docs.emplace_back(it->at<DOCID>());
      continue;
    }
  }

  if (erase_docs.size() > 0) {
    for (const std::string& docid : erase_docs) {
      doc_comment_table_.erase<DOCID>(docid);
    }
  }
  LOG(INFO) << "refresh doc_comment_table_ erase_docs_count:"
      << erase_docs.size();
  return 0;
}

int DocCommentIndexTable::Update(
  const std::map<std::string, int32_t>& doc_comments_num) {
  if (doc_comments_num.size() == 0) {
    return 0;
  }
  return UpdateCommentsNum(doc_comments_num);
}

int DocCommentIndexTable::UpdateCommentsNum(
    const std::map<std::string, int32_t>& doc_comments_num) {
  int64_t current_time = TimeUtils::GetCurrentTime();

  int comments_num_greater_than_0 = 0;  // 评论数大于 0 的数量
  std::string comments_num_greater_than_0_docid;
  for (const auto& item : doc_comments_num) {
    const std::string& doc_id = item.first;
    std::shared_ptr<feature_engine::DocComment> doc_comment(
        new feature_engine::DocComment);
    if (!doc_comment) {
      continue;
    }
    doc_comment->timestamp = current_time;
    doc_comment->comment_num = item.second;

    doc_comment_table_.insert(doc_id, doc_comment);
    if (doc_comment->comment_num > 0) {
      ++comments_num_greater_than_0;
      if (comments_num_greater_than_0 == 1) {
        // 评论数大于 0 的docid
        comments_num_greater_than_0_docid = doc_id;
      }
    }
  }
  LOG(INFO) << "[UpdateCommentsNum] comments_num_total:"
      << doc_comments_num.size()
      << " gt0_num:" << comments_num_greater_than_0
      << " gt0_docid:" << comments_num_greater_than_0_docid;
  return 0;
}

}  // namespace feature_engine
