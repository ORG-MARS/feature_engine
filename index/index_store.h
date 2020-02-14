
#ifndef FEATURE_ENGINE_INDEX_INDEX_STORE_H_
#define FEATURE_ENGINE_INDEX_INDEX_STORE_H_

#include <map>
#include <vector>
#include <string>
#include <memory>
#include <math.h>

#include <cstdlib>
#include <iostream>
//#include "feature_engine/index/redis_adapter.h"
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

class IndexStore {
 public:
  IndexStore();
  ~IndexStore();

  int Init();
  void Start();

  int GetAllDocsFromMysql(std::vector<std::shared_ptr<Document>>* docs);
  std::shared_ptr<MysqlConf> MakeMysqlConf();

  int GetDocumentsFromMysql(std::shared_ptr<MysqlConnection> c, std::string doc_type, std::vector<std::string> &json_strings);
  int GetDocumentsFromJsonParallel(const std::vector<std::string>& json_strings,
      std::vector<std::shared_ptr<Document>>* docs);
  int GetDocumentsFromJson(const std::vector<std::string>& json_strings,
      int start, int end, std::vector<std::shared_ptr<Document>>* docs);
  int LoadjsonFromFileParallel(std::vector<std::shared_ptr<Document>>* docs) ;
  int LoadJsonFromFile(const std::string& docinfo_index_file,
                int file_num, std::vector<std::shared_ptr<Document>>* docs);
  std::shared_ptr<Document> GetDocumentFromJson(const std::string& json_string);
  
};

}

#endif
