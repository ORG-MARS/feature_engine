// File   mysql_connection.h
// Author lidongming
// Date   2018-09-17 15:04:32
// Brief

#ifndef FEATURE_ENGINE_INDEX_MYSQL_CONNECTION_H_
#define FEATURE_ENGINE_INDEX_MYSQL_CONNECTION_H_

#include <string>
#include <memory>
#include <map>
#include "feature_engine/deps/mysql/include/mysql/mysql.h"
#include "feature_engine/deps/mysql/include/mysql/errmsg.h"

namespace feature_engine {

const char MYSQL_ENCODE[] = "utf8";

const int MYSQL_FIELD_LENGTH = 1048576;

struct MysqlConf {
  int port;
  std::string host;
  std::string user;
  std::string passwd;
  std::string database;
  MysqlConf() { }

  MysqlConf(int p, const std::string& h,
      const std::string& u,
      const std::string& pw,
      const std::string& db)
    : port(p), host(h), user(u), passwd(pw), database(db) {
    }
};

// struct MysqlField {
//     std::map<std::string, std::string> fields;
// };

class MysqlConnection {
 public:
  MysqlConnection();
  ~MysqlConnection();

  int Init(std::shared_ptr<MysqlConf> mysql_conf);

  int Connect();

  void Disconnect();

  int Query(const char* query);

  int ExecuteSql(const std::string& sql);

  const char* DBError();

  static int AddSlashes(MYSQL* mysql, const std::string& src_str,
      std::string* dst_str);

  MYSQL_RES* Result() { return mysql_store_result(mysql_); }

  void FreeResult(MYSQL_RES* result) {
    if (result) {
      mysql_free_result(result);
    }
  }

  int RowsCount(MYSQL_RES* result) { return mysql_num_rows(result); }

  MYSQL_ROW FetchRow(MYSQL_RES* result) {
    if (!result) { return MYSQL_ROW(); }
    return mysql_fetch_row(result);
  }

 protected:
  std::shared_ptr<MysqlConf> mysql_conf_;
  MYSQL* mysql_;
};

}

#endif
