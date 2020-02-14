// File   common_gflags.cpp
// Author lidongming
// Date   2018-08-28 11:48:40
// Brief

#include <thread>
#include "common/common_gflags.h"

namespace feature_engine {

// Server
DEFINE_int32(port, 11000, "server port");
DEFINE_int32(server_thread_count, 50, "server thead count");

// ThreadPool
DEFINE_int32(worker_thread_count, 50, "count of feature threads");
DEFINE_int32(docs_count_per_task, 100, "count of docs in one task");
DEFINE_int32(max_tasks_count, 50, "max count of tasks");
DEFINE_double(tasks_factor, 0.5, "tasks count factor");

// FIXME(lidongming):check load
// 1.5 * cpus
DEFINE_int32(max_task_queue_size, 70, "max task count in threadpool");
DEFINE_string(business_list, "toutiao,channels,video", "");

// Loader
DEFINE_int32(load_worker_thread_count, 20, "read json value of one doc from redis");
DEFINE_int32(load_task_count, 20, "load task count");
DEFINE_int32(mysql_load_worker_thread_count, 20, "read json value of one doc from redis");
DEFINE_int32(mysql_load_task_count, 20, "load task count");

// Monitor
DEFINE_string(monitor_status, "./data/monitor_status", "monitor status");
DEFINE_string(monitor_data, "./data/monitor.dat", "monitor data");
DEFINE_int32(monitor_interval, 60, "monitor interval");
DEFINE_int32(reload_interval, 10, "reload interval");

// Feature Cache
DEFINE_bool(enable_up_feature_cache, false, "");
DEFINE_bool(enable_doc_feature_cache, false, "");
DEFINE_int32(feature_cache_size, 500000, "");
DEFINE_int32(doc_feature_cache_size, 500000, "");
DEFINE_int32(up_feature_cache_size, 1000000, "");
DEFINE_int32(feature_cache_expire_seconds, 180, "");

DEFINE_int32(up_feature_cache_expire_seconds, 180, "");
DEFINE_int32(doc_feature_cache_expire_seconds, 180, "");
DEFINE_int32(feature_buffer_count, std::thread::hardware_concurrency(), "");
DEFINE_int32(feature_buffer_size, 100000, "");
DEFINE_int32(feature_cache_max_size, 500000, "");
DEFINE_int32(feature_cache_monitor_interval, 10000, "");
DEFINE_int32(tls_queue_update_size, 10000, "");
DEFINE_int32(tls_queue_update_interval, 10000, "");
DEFINE_int32(tls_queue_item_set_size, 10000, "");
DEFINE_int32(tls_queue_missing_update_size, 10000, "");
DEFINE_int32(tls_queue_missing_update_interval, 1000, "");
DEFINE_int32(tls_queue_missing_item_set_size, 10000, "");

// Redis
DEFINE_string(redis_host, "127.0.0.1", "redis host");
DEFINE_int32(redis_port, 6379, "redis port");
DEFINE_string(redis_docid_prefix, "ctr_", "docid prefix in redis");
DEFINE_int32(redis_mget_limit, 1000, "");
DEFINE_bool(enable_redis_cluster, true, "redis cluster");
DEFINE_string(redis_password, "redispassword", "redis password");
DEFINE_int32(redis_timeout_ms, 1000, "redis timeout ms");

DEFINE_string(disc_interest_redis_host, "127.0.0.1:6600", "");
DEFINE_string(disc_interest_redis_key, "discrete-interest", "");
DEFINE_string(disc_interest_redis_tm_key, "discrete-interest-tm", "");

// Stat redis
DEFINE_string(stat_disc_redis_host, "127.0.0.1:6600", "");
DEFINE_string(stat_disc_redis_key, "discrete-numeric", "");
DEFINE_string(stat_disc_redis_tm_key, "discrete-numeric-tm", "");

// Doc stat redis
DEFINE_string(doc_stat_redis_host, "127.0.0.1:6600", "");
DEFINE_int32(max_doc_stat_count, 1000000, "");
DEFINE_int32(max_doc_comment_count, 1000000, "");

DEFINE_int32(max_mget_count, 1000, "");

// Kafka
DEFINE_string(compression, "", "");
DEFINE_string(auto_commit, "", "");
DEFINE_string(commit_interval, "1000", "");
DEFINE_string(broker, "bjstream2.dg.163.org:9092,bjstream3.dg.163.org:9092,bjstream4.dg.163.org:9092", "");
DEFINE_string(topic, "datacenter_bjrec_stream_online3", "");
DEFINE_string(offset_type, "end", "");
DEFINE_int64(offset, 0, "");
DEFINE_int32(partition_count, 1, "");
DEFINE_bool(enable_kafka, true, "enable kafka");

// Index
DEFINE_bool(rebuild_total_index, false, "rebuild index from mysql and redis");
DEFINE_int32(update_index_interval, 60, "inc index interval");
DEFINE_int32(eliminate_index_interval, 60, "eliminate index interval");
DEFINE_int32(monitor_index_interval, 10, "monitor index interval");
DEFINE_int32(dump_index_interval, 60, "dump index interval");
DEFINE_int32(bucket_size,  0,  "st table bucket size");
//DEFINE_string(load_index_path,  "./data/index",  "load index path");

DEFINE_string(load_index_path,  "/home/appops/models/get_json_from_mysql/index",  "load index path");
DEFINE_string(dump_index_path,  "./data/index",  "dump index path");
DEFINE_int32(dump_count_per_file, 1000000, "dump into per file");
DEFINE_string(docinfo_file, "docinfo.idx", "docinfo index file");
DEFINE_int32(doc_limit, 500000, "doc limit");
DEFINE_int32(safe_freeze_time, 2, "safe freeze time");
DEFINE_double(load_factor, 0.1, "load factor");
DEFINE_bool(update_missing, true, "");
DEFINE_int32(update_missing_interval, 60, "seconds");
DEFINE_int32(doc_stat_expire_seconds, 180, "seconds");
DEFINE_int32(max_doc_stat_priority, 10, "");
DEFINE_int32(missing_docids_queue_size, 100, "");
DEFINE_int32(doc_comment_expire_seconds, 180, "seconds");
DEFINE_int32(max_doc_comment_priority, 10, "");
DEFINE_int32(update_comment_interval, 10, "seconds");
DEFINE_int32(updating_comment_docids_queue_size, 100, "");
DEFINE_bool(skip_invalid_doc, false, "");
DEFINE_int32(common_docs_limit, 1000000, "");
DEFINE_int32(video_docs_limit, 1000000, "");
DEFINE_int32(short_video_docs_limit, 1000000, "");

// Feature
DEFINE_string(feature_conf_path,  "./conf/features",  "features conf");

// Mysql
DEFINE_int32(max_mysql_query_retry, 3, "mysql query retry times");
DEFINE_string(mysql_host, "10.172.42.246", "");
DEFINE_int32(mysql_port, 3306, "");
DEFINE_string(mysql_user, "bjnewsrec", "");
DEFINE_string(mysql_passwd, "ywOJNZbzw", "");
DEFINE_string(mysql_database, "recommend", "");
DEFINE_string(mysql_table, "articles_streams", "");
DEFINE_int32(mysql_page_size, 1024, "page size");
DEFINE_string(mysql_query_fields, "doc_id,expire_time", "");
DEFINE_bool(enable_mysql, true, "enable mysql");

// Debug
DEFINE_int32(max_debug_mysql_rows_count, 10000, "");
DEFINE_int32(max_debug_mysql_docs_count, 1000, "");
DEFINE_bool(debug, false, "debug or not");
DEFINE_bool(load_reserved_docs, false, "load reserved docs");
DEFINE_string(reserved_docs, "./data/reserved_docs.dat", "reserved docs");
DEFINE_bool(dump_features, false, "dump features or not");
DEFINE_bool(click_debug, false, "debug click");
// DEFINE_bool(gen_textual_feature, false, "");
DEFINE_int32(textual_features_count, 1, "");
DEFINE_bool(print_features, false, "");
DEFINE_bool(debug_get_doc_property_comment, false, "");
//
//
DEFINE_string(self_server_ip, "localhost", "host ip");
DEFINE_string(consul_register_name, "feature_engine", "feature_engine server name");
DEFINE_int32(consul_server_check_ttl_sec, 10, "consul check ttl sec");
//consul client
DEFINE_string(consul_client_mode, "register", "register|deregister");

//http server   default is test http server
DEFINE_string(http_server_host, "10.122.216.86", "get doc from redis by http");
DEFINE_string(http_server_port, "8099", "get doc from redis by http");
DEFINE_string(http_server_target, "/api/material/query", "get doc from redis by http");
DEFINE_int32(http_docid_count_limit, 50, "get doc count one time from redis by http");

}  // namespace feature_engine
