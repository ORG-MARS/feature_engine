// File   index_table.h
// Author lidongming
// Date   2018-09-05 14:11:44
// Brief

#ifndef FEATURE_ENGINE_INDEX_INDEX_TABLE_H_
#define FEATURE_ENGINE_INDEX_INDEX_TABLE_H_

#include <string>
#include <memory>
#include <vector>
#include <unordered_map>

#include "feature_engine/index/document.h"
#include "feature_engine/index/index_table_define.h"

namespace feature_engine {

class IndexEntry;

// WARNING:Not thread-safe
class IndexTable {
public:
    IndexTable();
    ~IndexTable();

    int Init(IndexEntry* index_entry);
    int InitTables();

    int LoadDocinfoFile(const std::string& docinfo_index_file, int file_num,
    std::vector<std::shared_ptr<Document>>* docs);
    int Load();
    int Reload() { return Load(); }

    int Update(std::vector<std::shared_ptr<Document>>& update_batch);

    std::shared_ptr<Document> UpdateDocument(const std::string& docid,
            std::shared_ptr<Document> update_doc);

    std::shared_ptr<Document> GetDocument(const std::string& docid);
    inline int GetDocumentsSize() { return docinfo_table_.size(); }

    bool CheckDocument(const Document& d);

    // bool Filter(Context& context, Query& query, const Document& doc);

    int Eliminate();

    void DumpIndex();

    // Clear old index tables
    void ClearIndex();

    int WriteIntoFile(const std::string &docinfo_index_file_new,
                      const feature_proto::FeaturesList &feature_list);

    // Update inc index and re-rank index
    int RebuildIndex();

    // Builder
    int BuildIndex(std::vector<std::shared_ptr<Document>>& documents);

    void MonitorIndex();

private:
    // Forward index
    DocInfoTable docinfo_table_;

    IndexEntry* index_entry_;
};

}

#endif
