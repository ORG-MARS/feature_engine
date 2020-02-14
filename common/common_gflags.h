// File   common_gflags.h
// Author lidongming
// Date   2018-08-28 11:46:18
// Brief

#ifndef TFSERVER_COMMON_COMMON_GFLAGS_H_
#define TFSERVER_COMMON_COMMON_GFLAGS_H_

#include "gflags/gflags.h"

namespace feature_engine {

// Server
DECLARE_int32(port);
DECLARE_int32(server_thread_count);

// ThreadPool
DECLARE_int32(worker_thread_count);
DECLARE_int32(docs_count_per_task);
DECLARE_int32(max_task_queue_size);
DECLARE_int32(max_tasks_count);
DECLARE_double(tasks_factor);

DECLARE_string(business_list);

// Loader
DECLARE_int32(load_worker_thread_count);
DECLARE_int32(load_task_count);
DECLARE_int32(mysql_load_worker_thread_count);
DECLARE_int32(mysql_load_task_count);
DECLARE_int32(reload_interval);

// Monitor
DECLARE_string(monitor_status);
DECLARE_string(monitor_data);
DECLARE_int32(monitor_interval);

// Feature Cache
DECLARE_bool(enable_up_feature_cache);
DECLARE_bool(enable_doc_feature_cache);
DECLARE_int32(feature_cache_size);
DECLARE_int32(doc_feature_cache_size);
DECLARE_int32(up_feature_cache_size);
DECLARE_int32(feature_cache_expire_seconds);

DECLARE_int32(doc_feature_cache_expire_seconds);
DECLARE_int32(up_feature_cache_expire_seconds);
DECLARE_int32(feature_buffer_count);
DECLARE_int32(feature_buffer_size);
DECLARE_int32(feature_cache_max_size);
DECLARE_int32(tls_queue_update_size);
DECLARE_int32(tls_queue_update_interval);
DECLARE_int32(tls_queue_item_set_size);
DECLARE_int32(tls_queue_missing_update_size);
DECLARE_int32(tls_queue_missing_update_interval);
DECLARE_int32(tls_queue_missing_item_set_size);
DECLARE_int32(feature_cache_monitor_interval);

// Redis
DECLARE_string(redis_host);
DECLARE_int32(redis_port);
DECLARE_string(redis_docid_prefix);
DECLARE_int32(redis_mget_limit);
DECLARE_bool(enable_redis_cluster);
DECLARE_string(redis_password);
DECLARE_int32(redis_timeout_ms);

DECLARE_string(disc_interest_redis_host);
DECLARE_string(disc_interest_redis_key);
DECLARE_string(disc_interest_redis_tm_key);

DECLARE_int32(max_mget_count);
DECLARE_string(doc_stat_redis_host);
DECLARE_int32(max_doc_stat_count);
DECLARE_int32(max_doc_comment_count);

// Stat discrete range
DECLARE_string(stat_disc_redis_host);
DECLARE_string(stat_disc_redis_key);
DECLARE_string(stat_disc_redis_tm_key);

// Kafka
DECLARE_string(compression);
DECLARE_string(auto_commit);
DECLARE_string(commit_interval);
DECLARE_string(broker);
DECLARE_string(topic);
DECLARE_string(offset_type);
DECLARE_int64(offset);
DECLARE_int32(partition_count);
DECLARE_bool(enable_kafka);

// Index
DECLARE_bool(rebuild_total_index);
DECLARE_int32(update_index_interval);
DECLARE_int32(eliminate_index_interval);
DECLARE_int32(monitor_index_interval);
DECLARE_int32(dump_index_interval);
DECLARE_int32(bucket_size);
DECLARE_string(load_index_path);
DECLARE_string(dump_index_path);
DECLARE_int32(dump_count_per_file);
DECLARE_string(docinfo_file);
DECLARE_int32(doc_limit);
DECLARE_int32(safe_freeze_time);
DECLARE_double(load_factor);
DECLARE_bool(update_missing);
DECLARE_int32(update_missing_interval);
DECLARE_int32(doc_stat_expire_seconds);
DECLARE_int32(max_doc_stat_priority);
DECLARE_int32(missing_docids_queue_size);
DECLARE_int32(doc_comment_expire_seconds);
DECLARE_int32(max_doc_comment_priority);
DECLARE_int32(update_comment_interval);
DECLARE_int32(updating_comment_docids_queue_size);
DECLARE_bool(skip_invalid_doc);
DECLARE_int32(common_docs_limit);
DECLARE_int32(video_docs_limit);
DECLARE_int32(short_video_docs_limit);

// Feature
DECLARE_string(feature_conf_path);

// Mysql
DECLARE_int32(max_mysql_query_retry);
DECLARE_string(mysql_host);
DECLARE_int32(mysql_port);
DECLARE_string(mysql_user);
DECLARE_string(mysql_passwd);
DECLARE_string(mysql_database);
DECLARE_string(mysql_table);
DECLARE_int32(mysql_page_size);
DECLARE_string(mysql_query_fields);
DECLARE_bool(enable_mysql);

// Debug
DECLARE_int32(max_debug_mysql_rows_count);
DECLARE_int32(max_debug_mysql_docs_count);
DECLARE_bool(debug);
DECLARE_bool(load_reserved_docs);
DECLARE_string(reserved_docs);
DECLARE_bool(dump_features);
DECLARE_bool(click_debug);
// DECLARE_bool(gen_textual_feature);
DECLARE_int32(textual_features_count);
DECLARE_bool(print_features);
DECLARE_bool(debug_get_doc_property_comment);
//
DECLARE_int32(consul_server_check_ttl_sec);
DECLARE_string(consul_register_name);
DECLARE_string(self_server_ip);
DECLARE_string(consul_client_mode);

//http server   default is test http server
DECLARE_string(http_server_host);
DECLARE_string(http_server_port);
DECLARE_string(http_server_target);
DECLARE_int32(http_docid_count_limit);
}  // namespace feature_engine

#endif  // TFSERVER_COMMON_COMMON_GFLAGS_H_
