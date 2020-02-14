include "common_ml.thrift"
namespace cpp feature_thrift

enum FeatureType {
  LR_FEATURE,
  TF_FEATURE,
  ALGO_TF_FEATURE,
}

struct ExploreProfile {
  1: optional i64 reserved1;
}

struct FeatureRequest {
  1: required string rid;
  2: required FeatureType feature_type;
  3: optional ExploreProfile explore_profile;
  4: required list<i32> feature_ids;
  5: required list<common_ml.DocInfo> docs;
  6: required common_ml.UserInfo user_info;
  7: optional bool generate_textual_features;
  8: optional string project;
  // 代表何种业务, video/channels/toutiao/ai/algotf
  9: optional string business;
}

struct FeatureInfo {
  1: required i32 feature_id;
  //特征哈希值
  2: optional list<i64> hash_features;
  //特征值
  3: optional list<string> string_features;
  4: optional list<double> float_features;
  5: optional list<i64> number_features;
  //特征分数
  6: optional list<double> scores;
}

struct FeatureResponse {
  1: optional list<string> serialized_features;
  2: optional list<list<i64>> hash_features;
  3: optional list<list<string>> textual_features;
  4: optional list<map<i32,FeatureInfo>> features;
}

struct SearchDocRequest {
  1: required string rid;
  2: required string docid; 
  3: required list<i32> feature_ids; 
}

struct SearchDocResponse {
  1: optional i64 expire_time;
  2: optional map<i32, string> feature_map;
  3: optional i64 is_exist;
}

service FeatureService {
  FeatureResponse Features(1: FeatureRequest request)
  SearchDocResponse SearchDoc(1: SearchDocRequest request)
}

