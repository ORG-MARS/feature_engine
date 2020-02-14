// File   index_table_define.h
// Author lidongming
// Date   2018-09-05 14:01:37
// Brief

#ifndef FEATURE_ENGINE_INDEX_INDEX_COMMON_DEFINE_H_
#define FEATURE_ENGINE_INDEX_INDEX_COMMON_DEFINE_H_

#include <memory>
#include "feature_engine/deps/smalltable/include/smalltable.hpp"  // ST_TABLE
#include "feature_engine/index/document.h"    // Document
#include "feature_engine/common/common_define.h"  // MAKE_HASH

namespace feature_engine {

// Smalltable Declarations
//
// DEFINE_ATTRIBUTE(DOCID,      int64_t);
DEFINE_ATTRIBUTE(DOCID,      std::string);
DEFINE_ATTRIBUTE(DOCINFO,    std::shared_ptr<Document>);

DEFINE_ATTRIBUTE(DOC_STAT,  DocStat);
DEFINE_ATTRIBUTE(DOC_COMMENT,    std::shared_ptr<DocComment>);

typedef ST_TABLE(DOCID, DOCINFO, ST_UNIQUE_KEY(DOCID)) DocInfoTable;

typedef ST_TABLE(DOCID, DOC_STAT, ST_UNIQUE_KEY(DOCID)) DocStatTable;

typedef ST_TABLE(DOCID, DOC_COMMENT, ST_UNIQUE_KEY(DOCID)) DocCommentTable;

// IMPLEMENT: ADD INVERTED TABLE HERE

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_INDEX_INDEX_COMMON_DEFINE_H_
