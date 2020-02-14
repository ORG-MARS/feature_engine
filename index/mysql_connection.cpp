// File   mysql_connection.cpp
// Author lidongming
// Date   2018-09-17 15:06:10
// Brief

#include "feature_engine/index/mysql_connection.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/gflags/include/gflags/gflags.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

MysqlConnection::MysqlConnection()
  : mysql_conf_(NULL), mysql_(NULL) {
}

MysqlConnection::~MysqlConnection() {
  Disconnect();
}

int MysqlConnection::Init(std::shared_ptr<MysqlConf> mysql_conf) {
  mysql_conf_ = mysql_conf;
  return 0;
}

int MysqlConnection::Connect() {
  mysql_ = mysql_init(NULL);
  if (mysql_ == NULL) {
    LOG(FATAL) << "mysql_init error";
    return -1;
  }

  char value = 1;
  mysql_options(mysql_, MYSQL_SET_CHARSET_NAME, "utf8");
  mysql_options(mysql_, MYSQL_OPT_RECONNECT, (char*)&value);

  if (!mysql_real_connect(mysql_,
        mysql_conf_->host.c_str(), mysql_conf_->user.c_str(),
        mysql_conf_->passwd.c_str(), mysql_conf_->database.c_str(),
        mysql_conf_->port, NULL, 0)) {
    LOG(FATAL) << "mysql connect fail error:" << mysql_error(mysql_)
               << " host:" << mysql_conf_->host.c_str()
               << " user:" << mysql_conf_->user.c_str();
    mysql_close(mysql_);
    mysql_ = NULL;
    return -1;
  }

  // 设置数据库编码
  if (mysql_set_character_set(mysql_, "utf8") != 0) {
    LOG(WARNING) << "mysql set character set fail error:" << mysql_error(mysql_);
    mysql_close(mysql_);
    return -1;
  }
  return 0;
}

void MysqlConnection::Disconnect() {
  if (mysql_ != NULL) {
    mysql_close(mysql_);
    mysql_ = NULL;
  }
}

int MysqlConnection::Query(const char* query) {
  if (query == NULL) {
    LOG(WARNING) << "query is NULL";
    return -2;
  }

  if (mysql_ == NULL) {
    if (Connect()) {
      LOG(WARNING) << "Connect mysql error";
      return -1;
    }
  }

  int ret = mysql_query(mysql_, query);

  // 如果连接出错，则重新连接
  if (ret == CR_SERVER_GONE_ERROR || ret == CR_SERVER_LOST) {
    LOG(WARNING) << "mysql_query error, reconnect mysql after 1 sec";
    sleep(1);

    if (mysql_ping(mysql_) != 0) {
      Disconnect();
      if (!Connect()) {
        LOG(WARNING) << "reconnect db error";
        return -1;
      }
    }

    ret = mysql_query(mysql_, query);
    if (ret == CR_SERVER_GONE_ERROR || ret == CR_SERVER_LOST) {
      ret = -1;
    } else {
      ret = -3;
    }
  }

  return ret;
}

int MysqlConnection::ExecuteSql(const std::string& sql) {
  int retval = 0;
  int retry = 0;
  while (retry < FLAGS_max_mysql_query_retry) {
    retval = Query(sql.c_str());
    if (retval == 0) { break; }
    retry++;
    // LOG(WARNING) << "retry mysql query retry:%d ret:%d sql:%s", retry, retval, sql.c_str();
  }

  if (retval != 0) {
    // LOG_ERROR("mysql query failed ret:%d sql:%s", retval, sql.c_str());
    return -1;
  }

  // MYSQL_RES* result = Result();
  // if (result == NULL) {
  //     LOG_ERROR("invalid mysql result sql:%s", sql.c_str());
  //     return -1;
  // }
  // int rows_count = RowsCount(result);
  // if (rows_count == 0) {
  //     LOG_ERROR("no msg_id found sql:%s", sql.c_str());
  //     FreeResult(result);
  //     return -1;
  // }
  return 0;
}

const char* MysqlConnection::DBError() {
  return mysql_error(mysql_);
}

int  MysqlConnection::AddSlashes(MYSQL* mysql, const std::string& src_str,
    std::string* dst_str) {
  char buf[MYSQL_FIELD_LENGTH];
  if ((unsigned int)MYSQL_FIELD_LENGTH < (src_str.size() * 2 + 1)) {
    LOG(WARNING) << "mysql field too long";
    return -1;
  }

  mysql_real_escape_string(mysql, buf, src_str.c_str(), src_str.size());
  dst_str->assign(buf);
  return 0;
}

}
