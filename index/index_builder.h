// File   index_builder.h
// Author lidongming
// Date   2018-09-17 15:13:24
// Brief

#ifndef FEATURE_ENGINE_INDEX_BUILDER_INDEX_BUILDER_H_
#define FEATURE_ENGINE_INDEX_BUILDER_INDEX_BUILDER_H_

#include <map>
#include <vector>
#include <string>
#include <memory>
#include <math.h>

#include <boost/algorithm/string.hpp>
// #include "feature_engine/deps/commonlib/include/redis_cluster_client.h"
#include "feature_engine/index/redis_adapter.h"
#include "feature_engine/index/mysql_connection.h"
#include "feature_engine/index/document.h"

namespace feature_engine {

// Macros
// 初始化MysqlConf
#define MAKE_MYSQL_CONF(_prefix_)                                          \
   std::make_shared<MysqlConf>(                                            \
           config_t::get_instance().get_int(_prefix_##_MYSQL_PORT),        \
           config_t::get_instance().get_string(_prefix_##_MYSQL_HOST),     \
           config_t::get_instance().get_string(_prefix_##_MYSQL_USER),     \
           config_t::get_instance().get_string(_prefix_##_MYSQL_PASSWD),   \
           config_t::get_instance().get_string(_prefix_##_MYSQL_DATABASE))

class IndexEntry;

class IndexBuilder {
 public:
  IndexBuilder();
  ~IndexBuilder();

  int Init();
  void Start();

  int GetDocidsFromMysql(std::vector<std::string>* docids);
  int GetDocidsFromMysql(std::shared_ptr<MysqlConnection> c,
              const std::string& table_name, std::vector<std::string>* docids);

  int GetDocidsFromMysql(std::shared_ptr<MysqlConnection> c,
              const std::string& table_name, std::string doc_type,
              std::vector<std::string>* docids);

  int GetDocIDs(std::vector<std::string>* docids);
  int GetRowsCount(const std::string& table_name);
  int GetRowsCount(const std::string& table_name,
                   std::shared_ptr<MysqlConnection> c);

  std::vector<std::string> GetFieldNames(const std::string& table_name,
      std::shared_ptr<MysqlConnection> c);

  int GetFieldsFromMysql(std::shared_ptr<MysqlConnection> c,
      const std::string& table_name_prefix, std::vector<std::string>* docids);

  std::shared_ptr<MysqlConf> MakeMysqlConf();

  int GetDocumentsFromRedisParallel(const std::vector<std::string>& docids,
      std::vector<std::shared_ptr<Document>>* docs);

  int GetDocumentsFromRedis(const std::vector<std::string>& docids,
      int start, int end, std::vector<std::shared_ptr<Document>>* docs);

  std::shared_ptr<Document> GetDocumentFromRedis(RedisAdapter& client,
      const std::string& docid);

 private:
  // DISALLOW_COPY_AND_ASSIGN(IndexBuilder);
  
};

}

#endif
