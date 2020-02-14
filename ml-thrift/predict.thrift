include "common_ml.thrift"
include "feature.thrift"
namespace cpp prediction_thrift

enum PredictType {
  LR,
  TF,
  HASH,
  WEIGHT,
}

struct PredictOptions {
  1: optional i32 force_abtestid;
  2: optional string force_business;
  3: optional bool force_generate_textual_features;
  4: optional bool force_diff_features;
}

struct PredictRequest {
  1: required string rid;
  2: optional PredictType predict_type;
  3: optional list<map<i32, i64>> feature_hashes;
  4: optional list<map<i32, double>> feature_weights;

  7: optional list<common_ml.DocInfo> docs;
  8: optional common_ml.UserInfo user_info;
  9: optional string project;
  10: optional bool is_video;
  11: optional PredictOptions options;
}

struct PredictResult {
  1: optional string docid;
  2: optional i32 pctr;
  3: optional double float_pctr;
}

struct PredictResponse {
  1: optional i32 err_code;
  2: optional string msg;
  3: optional list<PredictResult> predict_results;
  4: optional feature.FeatureResponse feature_response;
}

// Prediction
service PredictService {
  PredictResponse Predict(1: PredictRequest request)
}
