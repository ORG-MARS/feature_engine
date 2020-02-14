// File   index_builder.cpp
// Author lidongming
// Date   2018-09-17 17:35:04
// Brief

#include "feature_engine/index/index_builder.h"
#include "feature_engine/index/redis_adapter.h"
#include "feature_engine/deps/glog/include/glog/logging.h"
#include "feature_engine/deps/boost/include/boost/algorithm/string/join.hpp"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/thread_pool.h"
// #include "feature_engine/deps/commonlib/include/redis_cluster_client.h"
#include "feature_engine/common/common_gflags.h"

namespace feature_engine {

using namespace commonlib;

IndexBuilder::IndexBuilder() { }

IndexBuilder::~IndexBuilder() { }

int IndexBuilder::Init() { return 0; }

int IndexBuilder::GetDocidsFromMysql(std::vector<std::string>* docids) {
  LOG(INFO) << "start get docs from mysql";
  std::shared_ptr<MysqlConf> mysql_conf = MakeMysqlConf();
  std::vector<std::future<int>> results;
  std::vector<std::string> res;

  // Init mysql connection
  std::shared_ptr<MysqlConnection> c = std::make_shared<MysqlConnection>();
  c->Init(mysql_conf);
  if (c->Connect() != 0) {
    LOG(WARNING) << "connect mysql error";
    return -1;
  }

  GetDocidsFromMysql(c, FLAGS_mysql_table, &res);
  docids->insert(docids->end(), res.begin(), res.end());

  LOG(INFO) << "finish get docs from mysql count:" << docids->size();
  return 0;
}

// 文章类型
// | video        |
// | doc          |
// | photoset     |
// | special      |
// | live         |
// | luobo        |
// | videospecial |
// | mint         |
// | shortvideo   |
// | wcteam       |
// | opencourse   |
// | motif        |
// | hamletmotif  |
// | videoalbum   |
// | rec          |          
int IndexBuilder::GetDocidsFromMysql(std::shared_ptr<MysqlConnection> c,
                          const std::string& table_name,
                          std::vector<std::string>* docids) {
  GetDocidsFromMysql(c, table_name, "doc", docids);
  GetDocidsFromMysql(c, table_name, "video", docids);
  GetDocidsFromMysql(c, table_name, "shortvideo", docids);
  return 0;
}

int IndexBuilder::GetDocidsFromMysql(std::shared_ptr<MysqlConnection> c,
                                  const std::string& table_name,
                                  std::string doc_type,
                                  std::vector<std::string>* docids) {
  time_t now = time(NULL);
  char tmp[64];
  strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&now));
  std::string sql = "select " + FLAGS_mysql_query_fields + " from " + table_name
                  + " WHERE expire_time > '" + tmp
                  + "' AND status = 0 AND origion in ('apollo','def')";
  if (doc_type == "video") {
    sql += " and skip_type = 'video' limit " + FLAGS_video_docs_limit;
  } else if (doc_type == "shortvideo") {
    sql += " and skip_type = 'shortvideo' limit " + FLAGS_short_video_docs_limit;
  } else if (doc_type == "doc") {
    sql += " and skip_type = 'doc' limit " + FLAGS_common_docs_limit;
  }

  if (c->ExecuteSql(sql) != 0) {
    LOG(WARNING) << "execute sql error sql:" << sql;
    return -1;
  }

  MYSQL_RES* result = c->Result();
  if (result == NULL) {
    LOG(WARNING) << "invalid mysql result sql" << sql;
    return -1;
  }

  int64_t current_time = commonlib::TimeUtils::GetCurrentTime() / 1000;
  int64_t expire_time = 0;
  int64_t expire_time2 = 0;
  std::string docid;
  MYSQL_ROW row;
  int count = 0;

  std::vector<std::string> fields;
  commonlib::StringUtils::Split(FLAGS_mysql_query_fields, ",", fields);

  while (row = c->FetchRow(result)) {
    if (FLAGS_debug && count >= FLAGS_max_debug_mysql_docs_count) {
      break;
    }
    docid = "";
    expire_time = 0;
    for (size_t i = 0; i < fields.size(); i++) {
      if (row[i] != NULL) {
        // Check docid
        if (fields[i] == "doc_id") {
          docid = row[i];
        }

        // Check expire
        if (fields[i] == "expire_time") {
          expire_time = commonlib::TimeUtils::DateToTimestamp(row[i]);
        }
        if (fields[i] == "expire_time2") {
          expire_time2 = commonlib::TimeUtils::DateToTimestamp(row[i]);
        }
      }
    }
    // if (expire_time == 0) {
      // LOG(INFO) << "invalid expire_time in mysql docid:" << docid;
    // }

    if (expire_time < expire_time2) {
      expire_time = expire_time2;
    }

    // Filter invalid or expire doc
    if (docid.empty() || expire_time <= current_time) {
      continue;
    }
    docids->emplace_back(docid);
    count++;
  }
  LOG(INFO) << "execute sql:" << sql << " count:" << count;

  c->FreeResult(result);
  LOG(INFO) << "finish get docids from mysql doc_type:" << doc_type
            << " count:" << count;
  return 0;
}

int IndexBuilder::GetDocIDs(std::vector<std::string>* docids) {
  std::shared_ptr<MysqlConf> mysql_conf = MakeMysqlConf();
  std::shared_ptr<MysqlConnection> c = std::make_shared<MysqlConnection>();
  c->Init(mysql_conf);
  if (c->Connect() != 0) {
    LOG(WARNING) << "connect mysql error";
    return -1;
  }

  // FIXME(lidongming):add monitor
  LOG(INFO) << "start get docids from mysql";
  GetFieldsFromMysql(c, FLAGS_mysql_table, docids);
  LOG(INFO) << "finish get docids from mysql";

  return 0;
}

int IndexBuilder::GetRowsCount(const std::string& table_name) {
  if (FLAGS_debug) {
    return FLAGS_max_debug_mysql_rows_count;
  }
  std::shared_ptr<MysqlConf> mysql_conf = MakeMysqlConf();
  std::shared_ptr<MysqlConnection> conn = std::make_shared<MysqlConnection>();
  conn->Init(mysql_conf);
  if (conn->Connect() != 0) {
    LOG(WARNING) << "connect mysql error";
    return -1;
  }
  return GetRowsCount(table_name, conn);
}

// 获取数据总行数，用于分页请求Mysql表
int IndexBuilder::GetRowsCount(const std::string& table_name,
    std::shared_ptr<MysqlConnection> c) {
  if (c == NULL) { return -1; }

  std::string sql = "select count(*) from " + table_name + ";";
  int retval = c->ExecuteSql(sql);
  if (retval != 0) {
    LOG(WARNING) << "get rows count error";
    return -1;
  }

  MYSQL_RES* result = c->Result();
  if (result == NULL) {
    LOG(WARNING) << "invalid mysql result sql:" << sql;
    return -1;
  }

  int rows_count = c->RowsCount(result);
  if (rows_count == 0) {
    LOG(WARNING) << "no result returned sql:" << sql;
    c->FreeResult(result);
    return -1;
  }

  MYSQL_ROW row = c->FetchRow(result);
  int ret_val = -1;
  if (row) { 
    ret_val = atoi(row[0]);
  }
  c->FreeResult(result);
  LOG(INFO) << "table name:" << table_name <<  "rows_count:" << ret_val;
  return ret_val;
}

// Query mysql meta data and get all the field/column names of a given table;
std::vector<std::string> IndexBuilder::GetFieldNames(
    const std::string& table_name,
    std::shared_ptr<MysqlConnection> c) {
  std::vector<std::string> result;
  if (c == NULL) { return result; }
  std::string sql
    = "select column_name from information_schema.columns where table_name=\'"
    + table_name + "\';";
  int retval = c->ExecuteSql(sql);
  if (retval != 0) {
    LOG(WARNING) << "get field names error, table name is:" << table_name;
    return result;
  }
  MYSQL_RES* mysql_result = c->Result();
  if (mysql_result == NULL) {
    LOG(WARNING) << "invalid mysql result sql:%s", sql.c_str();
    return result;
  }
  for (MYSQL_ROW mysql_row = c->FetchRow(mysql_result); mysql_row;
      mysql_row = c->FetchRow(mysql_result)) {
    if (mysql_row[0] == NULL) {
      LOG(WARNING) << "invalid row when getting field names";
      c->FreeResult(mysql_result);
      return result;
    } else {
      result.push_back(mysql_row[0]);
    }       
  }
  c->FreeResult(mysql_result);
  return result;
}

int IndexBuilder::GetFieldsFromMysql(std::shared_ptr<MysqlConnection> c,
    const std::string& table_name, std::vector<std::string>* docids) {
  // int rows_count = GetRowsCount(table_name, c); 
  int rows_count = 0;
  int mysql_page_size = FLAGS_mysql_page_size;
  if (FLAGS_debug) {
    rows_count = FLAGS_max_debug_mysql_rows_count;;
    mysql_page_size = rows_count;
  } else {
    rows_count = GetRowsCount(table_name, c); 
  }
  if (rows_count <= 0) {
    LOG(WARNING) << "no rows in table:" << table_name;
    return -1;
  }
  LOG(INFO) << "rows count:" << rows_count;

  // Get all fields of one table
  // std::vector<std::string> fields = GetFieldNames(table_name, c);
  // if (fields.empty()) { return -1; }
  // std::string query_fields = boost::algorithm::join(fields, ",");

  // Only get doc_id and expire_time fields
  std::vector<std::string> fields = { "doc_id", "expire_time" };
  std::string query_fields = "doc_id,expire_time";

  std::string sql;

  for (int i = 0; i <= rows_count / mysql_page_size; i++) {
    sql = "select " + query_fields + " from " + table_name + " limit "
      + std::to_string(i * mysql_page_size) + ","
      + std::to_string(mysql_page_size) + ";";

    if (c->ExecuteSql(sql) != 0) {
      LOG(WARNING) << "execute sql error:" << sql;
      continue;
    }

    MYSQL_RES* result = c->Result();
    if (result == NULL) {
      LOG(WARNING) << "invalid mysql result sql" << sql;
      continue;
    }

    // FIXME(lidongming):
    int64_t current_time = commonlib::TimeUtils::GetCurrentTime() / 1000;
    int64_t expire_time = 0;
    std::string docid;
    MYSQL_ROW row;
    int count = 0;
    while (row = c->FetchRow(result)) {
      if (FLAGS_debug && count >= FLAGS_max_debug_mysql_docs_count) {
        break;
      }
      docid = "";
      expire_time = 0;
      for (size_t i = 0; i < fields.size(); i++) {
        if (row[i] == NULL) {
          // LOG(WARNING) << "field is null:" << fields[i];
          continue;
        }

        // Check docid
        if (fields[i] == "doc_id") {
          docid = row[i];
        }

        // Check expire
        // FIXME(lidongming):difference between expire_time and expire_time2?
        if (fields[i] == "expire_time") {
          expire_time = commonlib::TimeUtils::DateToTimestamp(row[i]);
        }
      }

      // Filter invalid or expire doc
      if (docid.empty() || expire_time <= current_time) {
        // LOG(INFO) << "skip docoument docid:" << docid
          // << " expire_time:" << expire_time
          // << " current_time:" << current_time;
        continue;
      } else {
        // LOG(INFO) << "update docoument docid:" << docid
        //           << " expire_time:" << expire_time
        //           << " current_time:" << current_time;
      }
      docids->emplace_back(docid);
      count++;
    }
    LOG(INFO) << "execute sql:" << sql << " i:" << i << " count:" << count;

    c->FreeResult(result);
  }
  return 0;
}

std::shared_ptr<MysqlConf> IndexBuilder::MakeMysqlConf() {
  return std::make_shared<MysqlConf>(
      FLAGS_mysql_port, FLAGS_mysql_host,
      FLAGS_mysql_user, FLAGS_mysql_passwd,
      FLAGS_mysql_database);
}

int IndexBuilder::GetDocumentsFromRedisParallel(
    const std::vector<std::string>& docids,
    std::vector<std::shared_ptr<Document>>* docs) {
  if (docids.empty()) {
    return -1;
  }

  ThreadPool load_thread_pool(FLAGS_load_worker_thread_count, AFFINITY_DISABLE, 0);

  int docids_count_per_task = std::ceil(docids.size() * 1.0 / FLAGS_load_task_count);
  int load_task_count = FLAGS_load_task_count;
  if (docids.size() < load_task_count) {
    load_task_count = docids.size();
    docids_count_per_task = 1;
  }
  std::vector<std::future<int>> results;
  int start = 0;
  int end = 0;
  for (int i = 0; i < load_task_count; i++) {
    start = i * docids_count_per_task;
    end = (i + 1) * docids_count_per_task;
    results.emplace_back(
        load_thread_pool.enqueue(
          std::bind(&IndexBuilder::GetDocumentsFromRedis, this, docids,
            start, end, docs)));
  }
  for (auto&& r : results) {
    r.get();
  }
  return 0;
}

// int mget(const std::string& prefix,
// const std::vector<std::string>& keys,
// std::unordered_map<std::string, std::string> & res);
int IndexBuilder::GetDocumentsFromRedis(
    const std::vector<std::string>& docids,
    int start, int end,
    std::vector<std::shared_ptr<Document>>* docs) {
  if (end > docids.size()) {
    end = docids.size();
  }

  RedisAdapter redis_adapter(FLAGS_redis_host, FLAGS_redis_port,
      FLAGS_redis_password, FLAGS_redis_timeout_ms);

  int retval = 0;
  int count = 0;
  int pos = start;

  int parse_success_count = 0;
  int parse_fail_count = 0;

  std::vector<std::string> keys;
  std::unordered_map<std::string, std::string> res;
  // for (const std::string& docid : docids) {
  for (int i = start; i < end; i++) {
    keys.emplace_back(docids[i]);
    if (count++ < FLAGS_redis_mget_limit && i != end - 1) {
      continue;
    }
    retval = redis_adapter.mget(FLAGS_redis_docid_prefix, keys, res);
    if (retval != 0) {
      LOG(WARNING) << "get docs from redis error";
      break;
    }
    // if (res.size() != keys.size()) {
    //   LOG(WARNING) << "size not equal res_size:" << res.size()
    //                << " keys_size:" << keys.size()
    //                << " diff:" << keys.size() - res.size();
    // }

    for (const std::string& docid : keys) {
      const std::string& json_value = res[docid];
      (*docs)[pos] = nullptr;
      if (json_value.empty()) {
        // LOG(WARNING) << "json value is empty docid:" << docid;
        parse_fail_count++;
      } else {
        std::shared_ptr<Document> document = std::make_shared<Document>();
        Status status = document->ParseFromJson(json_value);
        if (status.ok()) {
          (*docs)[pos] = std::move(document);
          parse_success_count++;
          // LOG(INFO) << "parse document ok docid:" << docid
          //           << " count:" << pos - start;
        } else {
          parse_fail_count++;
          LOG(WARNING) << "parse document from json value error docid:" << docid;
        }
      }
      pos++;
    }
    count = 0;
    keys.clear();
    res.clear();
  }

  // TODO(lidongming):add monitor
  LOG(INFO) << "parse document success count:" << parse_success_count;
  LOG(INFO) << "parse document fail count:" << parse_fail_count;

  return retval;
}

std::shared_ptr<Document> IndexBuilder::GetDocumentFromRedis(
    RedisAdapter& client, const std::string& docid) {
  std::string json_value;
  json_value = client.get(FLAGS_redis_docid_prefix + docid);
  // LOG(INFO) << "start get doc docid:" << FLAGS_redis_docid_prefix + docid;
  if (json_value.empty()) {
    // LOG(WARNING) << "json value is empty docid:" << docid;
    return nullptr;
  }
  std::shared_ptr<Document> document = std::make_shared<Document>();
  Status status = document->ParseFromJson(json_value);
  if (status.ok()) {
    // doc->reset(document);
    // *doc = std::move(document);
    return document;
  } else {
    LOG(WARNING) << "parse document from json value error";
    return nullptr;
  }
  return nullptr;
}

}
