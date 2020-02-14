// File   manual_method_parser.cpp
// Author lidongming
// Date   2018-09-10 23:15:00
// Brief

#include "math.h"
#include "feature_engine/parser/manual_method_parser.h"
#include "feature_engine/parser/parser_common.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include "feature_engine/deps/commonlib/include/string_utils.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/common/common_define.h"

namespace feature_engine {

using namespace commonlib;

// static const char kCombineSplitChar = char(0x01);
static const char kCombineSplitChar = '|';

// For PreNClickInterval
// TODO(lidongming):hard coding
static const std::vector<int> g_click_history_tm_vector = {
    2,5,10,30,60,180,600
};
static const std::vector<std::string> g_click_history_tm_str_vector = {
    "_2", "2_5", "5_10", "10_30", "30_60", "60_180", "180_600", "600_"
};
static const std::vector<uint64_t> g_click_history_tm_hash_vector = {
    MAKE_HASH("_2"), MAKE_HASH("2_5"),
    MAKE_HASH("5_10"), MAKE_HASH("10_30"),
    MAKE_HASH("30_60"), MAKE_HASH("60_180"),
    MAKE_HASH("180_600"), MAKE_HASH("600_")
};

#define MANUAL_METHOD_PARSER_DEFINITION(_name_) \
 ManualMethodParser::_name_( \
    bool generate_textual_feature, \
    const FeatureConf& feature_conf, \
    const DocStat& doc_stat, \
    FeatureMap* up_feature_map, \
    FeatureMap* cache_up_feature_map, \
    FeatureMap* doc_feature_map, \
    FeatureMap* instant_feature_map, \
    FeatureMap* cache_feature_map, \
    const common_ml_thrift::DocInfo* docinfo, \
    FeatureMap* feature_map)

// Static
// Category离散区间
std::shared_ptr<std::unordered_map<std::string, DiscInterest>>
  ManualMethodParser::disc_interest_ = nullptr;
// 统计特征离散区间
std::shared_ptr<std::unordered_map<std::string, StatDisc>>
  ManualMethodParser::stat_disc_ = nullptr;

// Single Feature Parser Method
Status MANUAL_METHOD_PARSER_DEFINITION(Simple) {
  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map,
                            cache_up_feature_map, doc_feature_map, feature_map,
                            cache_feature_map, FIRST_DEPEND, HASH_FEATURE,
                            depend_feature);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature hash_feature;
  switch(depend_feature->kind_case()) {
    case KVListUint64:
      {
        auto* list_hash = hash_feature.mutable_v_list_uint64();
        *list_hash = depend_feature->v_list_uint64();
      }
      break;
    case KVUint64:
      hash_feature.set_v_uint64(depend_feature->v_uint64());
      break;
    case KVUint32:
      hash_feature.set_v_uint32(depend_feature->v_uint32());
      break;
    default:
      return Status(error::FEATURE_PARSE_ERROR,
                    "unsupported depend hash feature type");
      break;
  }
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  if (generate_textual_feature) {
    // Generate Textual features
    status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                              doc_feature_map, feature_map, cache_feature_map,
                              FIRST_DEPEND, NOT_HASH_FEATURE, depend_feature);
    CHECK_STATUS_RETURN(status);

    feature_proto::Feature feature;
    switch(depend_feature->kind_case()) {
      case KVListString:
        {
          auto* list_string = feature.mutable_v_list_string();
          *list_string = depend_feature->v_list_string();
#ifndef NDEBUG
          for (int i = 0; i < list_string->k_size(); i++) {
            const std::string& v = list_string->k(i);
            FeatureLog<std::string>(feature_conf, v);
          }
#endif
        }
        break;
      case KVString:
        {
          feature.set_v_string(depend_feature->v_string());
          FeatureLog<std::string>(feature_conf, depend_feature->v_string());
        }
        break;
      case KVUint64:
        {
          std::string v = std::to_string(depend_feature->v_uint64());
          feature.set_v_string(v);
          FeatureLog<std::string>(feature_conf, v);
        }
        break;
      case KVUint32:
        {
          std::string v = std::to_string(depend_feature->v_uint32());
          feature.set_v_string(v);
          FeatureLog<std::string>(feature_conf, v);
        }
        break;
      case KVInt32:
        {
          std::string v = std::to_string(depend_feature->v_int32());
          feature.set_v_string(v);
          FeatureLog<std::string>(feature_conf, v);
        }
        break;

      default:
        return Status(error::FEATURE_PARSE_ERROR,
                      "unsupported depend feature type");
        break;
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }

  return Status::OK();
}

Status ManualMethodParser::TopN(bool generate_textual_feature,
                                const FeatureConf& feature_conf,
                                FeatureMap* up_feature_map,
                                FeatureMap* cache_up_feature_map,
                                FeatureMap* doc_feature_map,
                                FeatureMap* instant_feature_map,
                                FeatureMap* cache_feature_map,
                                FeatureMap* feature_map) {
  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVListUint64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map,
                            FIRST_DEPEND, HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);
  // Param already passed in feature_conf_parser
  if (unlikely(feature_conf.int_params.empty())) {
    return invalid_param_status;
  }
  int topn = feature_conf.int_params[0];
  if (unlikely(topn <= 0)) {
    return invalid_param_status;
  }

  if (topn > depend_feature->v_list_uint64().k_size()) {
    topn = depend_feature->v_list_uint64().k_size();
  }

  feature_proto::Feature hash_feature;
  auto* list_hash = hash_feature.mutable_v_list_uint64();
  for (int i = 0; i < topn; i++) {
    uint64_t v = depend_feature->v_list_uint64().k(i);
    list_hash->add_k(v);
    FeatureLog<uint64_t>(feature_conf, v);
  }
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  // Return if not generate textual feature
  if (generate_textual_feature) {
    // Generate Textual Feature
    status = GetDependFeature(feature_conf, KVListString, up_feature_map,
        cache_up_feature_map,
        doc_feature_map, instant_feature_map, cache_feature_map,
        FIRST_DEPEND, NOT_HASH_FEATURE, depend_feature);
    CHECK_STATUS_RETURN(status);

    feature_proto::Feature feature;
    auto* list_string = feature.mutable_v_list_string();

    for (int i = 0; i < topn; i++) {
      const std::string& v = depend_feature->v_list_string().k(i);
      list_string->add_k(v);
      FeatureLog<std::string>(feature_conf, v);
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(MultiKV1TopN) {
  return TopN(generate_textual_feature, feature_conf, up_feature_map,
              cache_up_feature_map, doc_feature_map, instant_feature_map,
              cache_feature_map, feature_map);
}

Status MANUAL_METHOD_PARSER_DEFINITION(MultiKV2TopN) {
  return TopN(generate_textual_feature, feature_conf, up_feature_map,
              cache_up_feature_map, doc_feature_map, instant_feature_map,
              cache_feature_map, feature_map);
}

Status MANUAL_METHOD_PARSER_DEFINITION(SegmentTopN) {
  return TopN(generate_textual_feature, feature_conf, up_feature_map,
              cache_up_feature_map, doc_feature_map, instant_feature_map,
              cache_feature_map, feature_map);
}

Status MANUAL_METHOD_PARSER_DEFINITION(MultiLevelDiscrete) {
  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVListUint64, up_feature_map,
                            cache_up_feature_map, doc_feature_map,
                            instant_feature_map, cache_feature_map,
                            FIRST_DEPEND, HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature hash_feature;
  auto* list_hash = hash_feature.mutable_v_list_uint64();
  for (int i = 0; i < depend_feature->v_list_uint64().k_size(); i++) {
    uint64_t v = depend_feature->v_list_uint64().k(i);
    list_hash->add_k(v);
    FeatureLog<uint64_t>(feature_conf, v);
  }
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  // Return if not generate textual feature
  if (generate_textual_feature) {
    status = GetDependFeature(feature_conf, KVListString, up_feature_map,
                              cache_up_feature_map, doc_feature_map,
                              instant_feature_map, cache_feature_map,
                              FIRST_DEPEND, NOT_HASH_FEATURE, depend_feature);
    CHECK_STATUS_RETURN(status);

    feature_proto::Feature feature;
    auto* list_string = feature.mutable_v_list_string();
    for (int i = 0; i < depend_feature->v_list_string().k_size(); i++) {
      const std::string& v = depend_feature->v_list_string().k(i);
      list_string->add_k(v);
      FeatureLog<std::string>(feature_conf, v);
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

// For UserAge
Status MANUAL_METHOD_PARSER_DEFINITION(IntDiscrete) {
  feature_proto::Feature* depend_feature = NULL;
  // UserAge(Input type)
  Status status;
  status = GetDependFeature(feature_conf, KVInt32, up_feature_map,
                            cache_up_feature_map, doc_feature_map,
                            instant_feature_map, cache_feature_map,
                            FIRST_DEPEND, NOT_HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  int value = depend_feature->v_int32();

  const std::vector<int>& int_params = feature_conf.int_params;
  const std::vector<std::string>& params = feature_conf.string_params;

  // discrate value
  int i = 0;
  for (i = 0; i < int_params.size(); ++i) {
    if (value <= int_params[i]) {
      break;
    }
  }
  std::string f;
  if (i == 0) {
    f = "_" + params[0];
  } else if (i >= params.size()) {
    f = params[params.size() - 1] + "_";
  } else {
    f = params[i - 1] + "_" + params[i];
  }

  feature_proto::Feature hash_feature;
  hash_feature.set_v_uint64(MAKE_HASH(f));
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  if (generate_textual_feature) {
    feature_proto::Feature feature;
    feature.set_v_string(f);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, f);
  }

  return Status::OK();
}

Status ManualMethodParser::Match(bool generate_textual_feature,
                                 bool match_length,
                                 const FeatureConf& feature_conf,
                                 FeatureMap* up_feature_map,
                                 FeatureMap* cache_up_feature_map,
                                 FeatureMap* doc_feature_map,
                                 FeatureMap* instant_feature_map,
                                 FeatureMap* cache_feature_map,
                                 FeatureMap* feature_map) {
  Status status = CheckDepends(feature_conf, 2);
  CHECK_STATUS_RETURN(status);

  // Check depend features f1 and f2
  feature_proto::Feature* f1 = NULL;
  status = GetDependFeature(feature_conf, KVListUint64, up_feature_map,
                            cache_up_feature_map, doc_feature_map,
                            instant_feature_map, cache_feature_map,
                            FIRST_DEPEND, HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KVListUint64, up_feature_map,
                            cache_up_feature_map, doc_feature_map,
                            instant_feature_map, cache_feature_map,
                            SECOND_DEPEND, HASH_FEATURE, f2);
  CHECK_STATUS_RETURN(status);

  int topn = atoi(feature_conf.params.c_str());
  // if params is not set, don't cut off
  if (0 == topn) {
    topn = f1->v_list_uint64().k_size();
  }
  if (unlikely(topn < 0)) {
    std::string msg = feature_conf.name + " invalid params";
    return Status(error::FEATURE_PARSE_ERROR, msg);
  }

  feature_proto::Feature feature;
  feature_proto::Feature hash_feature;
  feature_proto::ListUint64* list_hash = NULL;
  int match_count = 0;
  if (!match_length) {
    list_hash = hash_feature.mutable_v_list_uint64();
  }

  std::unordered_set<uint64_t> f2_set;
  for (int i = 0; i < f2->v_list_uint64().k_size(); i++) {
    f2_set.insert(f2->v_list_uint64().k(i));
  }
  for (int i = 0; i < f1->v_list_uint64().k_size() && match_count < topn; i++) {
    uint64_t v1 = f1->v_list_uint64().k(i);
    if (f2_set.find(v1) != f2_set.end()) {
      match_count++;
      if (!match_length) {
        list_hash->add_k(v1);
        FeatureLog<uint64_t>(feature_conf, v1);
      }
    }
  }
  if (match_length) {
    hash_feature.set_v_uint64(MAKE_HASH(std::to_string(match_count)));
    FeatureLog<int>(feature_conf, match_count);
  }
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  if (generate_textual_feature) {
    // Generate Textual Feature
    status = GetDependFeature(feature_conf, KVListString, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map,
                              FIRST_DEPEND, NOT_HASH_FEATURE, f1);
    CHECK_STATUS_RETURN(status);

    status = GetDependFeature(feature_conf, KVListString, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map,
                              SECOND_DEPEND, NOT_HASH_FEATURE, f2);
    CHECK_STATUS_RETURN(status);

    feature_proto::Feature feature;
    feature_proto::ListString* list_string = NULL;
    match_count = 0;
    if (!match_length) {
      list_string = feature.mutable_v_list_string();
    }

    std::unordered_set<std::string> f2_str_set;
    for (int i = 0; i < f2->v_list_string().k_size(); i++) {
      f2_str_set.insert(f2->v_list_string().k(i));
    }

    for (int i = 0; i < f1->v_list_string().k_size() && match_count < topn; i++) {
      const std::string& v1 = f1->v_list_string().k(i);
      if (f2_str_set.find(v1) != f2_str_set.end()) {
        match_count++;
        if (!match_length) {
          list_string->add_k(v1);
          FeatureLog<std::string>(feature_conf, v1);
        }
      }
    }
    if (match_length) {
      feature.set_v_string(std::to_string(match_count));
      FeatureLog<int>(feature_conf, match_count);
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

// Cross Feature Parser Method
Status MANUAL_METHOD_PARSER_DEFINITION(MatchTopic1) {
  return Match(generate_textual_feature, NOT_MATCH_LENGTH, feature_conf,
               up_feature_map, cache_up_feature_map, doc_feature_map,
               instant_feature_map, cache_feature_map, feature_map);
}

Status MANUAL_METHOD_PARSER_DEFINITION(MatchTopic2) {
  return Match(generate_textual_feature, NOT_MATCH_LENGTH, feature_conf,
               up_feature_map, cache_up_feature_map, doc_feature_map,
               instant_feature_map, cache_feature_map, feature_map);
}

Status MANUAL_METHOD_PARSER_DEFINITION(MatchTopic3) {
  return Match(generate_textual_feature, NOT_MATCH_LENGTH, feature_conf,
               up_feature_map, cache_up_feature_map, doc_feature_map,
               instant_feature_map, cache_feature_map, feature_map);
}

Status MANUAL_METHOD_PARSER_DEFINITION(MatchLength1) {
  Status status = Match(generate_textual_feature, MATCH_LENGTH, feature_conf,
                        up_feature_map, cache_up_feature_map, doc_feature_map,
                        instant_feature_map, cache_feature_map, feature_map);
  if (!status.ok()) {
    MakeZeroMatchLengthFeatures(feature_conf, feature_map);
  }
  return status;
}

Status MANUAL_METHOD_PARSER_DEFINITION(MatchLength2) {
  Status status = Match(generate_textual_feature, MATCH_LENGTH, feature_conf,
                        up_feature_map, cache_up_feature_map, doc_feature_map,
                        instant_feature_map, cache_feature_map, feature_map);
  if (!status.ok()) {
    MakeZeroMatchLengthFeatures(feature_conf, feature_map);
  }
  return status;
}

Status MANUAL_METHOD_PARSER_DEFINITION(MatchLength3) {
  Status status = Match(generate_textual_feature, MATCH_LENGTH, feature_conf,
                        up_feature_map, cache_up_feature_map, doc_feature_map,
                        instant_feature_map, cache_feature_map, feature_map);
  if (!status.ok()) {
    MakeZeroMatchLengthFeatures(feature_conf, feature_map);
  }
  return status;
}

Status MANUAL_METHOD_PARSER_DEFINITION(Combine2) {
  Status status = CheckDepends(feature_conf, 2);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f1 = NULL;
  status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map,
                            FIRST_DEPEND, HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map,
                            SECOND_DEPEND, HASH_FEATURE, f2);
  CHECK_STATUS_RETURN(status);


  feature_proto::Feature::KindCase f1_kind_case = f1->kind_case();
  feature_proto::Feature::KindCase f2_kind_case = f2->kind_case();

  if (f1_kind_case != KVListUint64 && f1_kind_case != KVUint64) {
    return invalid_type_status;
  }
  if (f2_kind_case != KVListUint64 && f2_kind_case != KVUint64) {
    return invalid_type_status;
  }

  std::vector<uint64_t> hash_values1;
  std::vector<uint64_t> hash_values2;

  if (f1_kind_case == KVUint64) {
    hash_values1.emplace_back(f1->v_uint64());
  } else {
    for (int i = 0; i < f1->v_list_uint64().k_size(); i++) {
      hash_values1.emplace_back(f1->v_list_uint64().k(i));
    }
  }

  if (f2_kind_case == KVUint64) {
    hash_values2.emplace_back(f2->v_uint64());
  } else {
    for (int i = 0; i < f2->v_list_uint64().k_size(); i++) {
      hash_values2.emplace_back(f2->v_list_uint64().k(i));
    }
  }

  feature_proto::Feature hash_feature;
  auto* list_hash = hash_feature.mutable_v_list_uint64();

  uint64_t hash_key = 0;
  for (uint64_t h1 : hash_values1) {
    for (uint64_t h2 : hash_values2) {
      uint64_t hash_value = GEN_HASH2(h1, h2);
      list_hash->add_k(hash_value);
      FeatureLog<uint64_t>(feature_conf, hash_value);
    }
  }
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  if (generate_textual_feature) {
    status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map,
                              FIRST_DEPEND, NOT_HASH_FEATURE, f1);
    CHECK_STATUS_RETURN(status);

    status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map,
                              SECOND_DEPEND, NOT_HASH_FEATURE, f2);
    CHECK_STATUS_RETURN(status);

    f1_kind_case = f1->kind_case();
    f2_kind_case = f2->kind_case();

    if (f1_kind_case != KVListString && f1_kind_case != KVString) {
      return invalid_type_status;
    }

    if (f2_kind_case != KVListString && f2_kind_case != KVString) {
      return invalid_type_status;
    }

    std::vector<std::string> feature_values1;
    std::vector<std::string> feature_values2;

    if (f1_kind_case == KVString) {
      feature_values1.emplace_back(f1->v_string());
    } else {
      for (int i = 0; i < f1->v_list_string().k_size(); i++) {
        feature_values1.emplace_back(f1->v_list_string().k(i));
      }
    }

    if (f2_kind_case == KVString) {
      feature_values2.emplace_back(f2->v_string());
    } else {
      for (int i = 0; i < f2->v_list_string().k_size(); i++) {
        feature_values2.emplace_back(f2->v_list_string().k(i));
      }
    }

    feature_proto::Feature feature;
    auto* list_string = feature.mutable_v_list_string();

    for (std::string& v1 : feature_values1) {
      for (std::string& v2 : feature_values2) {
        list_string->add_k(v1 + kCombineSplitChar + v2);
      }
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(Combine3) {
  Status status = CheckDepends(feature_conf, 3);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f1 = NULL;
  status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                            HASH_FEATURE, f2);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f3 = NULL;
  status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, THIRD_DEPEND,
                            HASH_FEATURE, f3);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature::KindCase f1_kind_case = f1->kind_case();
  feature_proto::Feature::KindCase f2_kind_case = f2->kind_case();
  feature_proto::Feature::KindCase f3_kind_case = f3->kind_case();

  if (f1_kind_case != KVListUint64 && f1_kind_case != KVUint64) {
    return invalid_type_status;
  }
  if (f2_kind_case != KVListUint64 && f2_kind_case != KVUint64) {
    return invalid_type_status;
  }
  if (f3_kind_case != KVListUint64 && f3_kind_case != KVUint64) {
    return invalid_type_status;
  }

  std::vector<uint64_t> hash_values1;
  std::vector<uint64_t> hash_values2;
  std::vector<uint64_t> hash_values3;

  if (f1_kind_case == KVUint64) {
    hash_values1.emplace_back(f1->v_uint64());
  } else {
    for (int i = 0; i < f1->v_list_uint64().k_size(); i++) {
      hash_values1.emplace_back(f1->v_list_uint64().k(i));
    }
  }

  if (f2_kind_case == KVUint64) {
    hash_values2.emplace_back(f2->v_uint64());
  } else {
    for (int i = 0; i < f2->v_list_uint64().k_size(); i++) {
      hash_values2.emplace_back(f2->v_list_uint64().k(i));
    }
  }

  if (f3_kind_case == KVUint64) {
    hash_values3.emplace_back(f3->v_uint64());
  } else {
    for (int i = 0; i < f3->v_list_uint64().k_size(); i++) {
      hash_values3.emplace_back(f3->v_list_uint64().k(i));
    }
  }

  feature_proto::Feature hash_feature;
  auto* list_hash = hash_feature.mutable_v_list_uint64();
  for (uint64_t h1 : hash_values1) {
    for (uint64_t h2 : hash_values2) {
      uint64_t hash_key = GEN_HASH2(h1, h2);
      for (uint64_t h3 : hash_values3) {
        list_hash->add_k(GEN_HASH2(hash_key, h3));
      }
    }
  }

  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  if (generate_textual_feature) {
    status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                              NOT_HASH_FEATURE, f1);
    CHECK_STATUS_RETURN(status);

    status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                              NOT_HASH_FEATURE, f2);
    CHECK_STATUS_RETURN(status);

    status = GetDependFeature(feature_conf, KV_KIND_NOT_SET, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map, THIRD_DEPEND,
                              NOT_HASH_FEATURE, f3);
    CHECK_STATUS_RETURN(status);

    f1_kind_case = f1->kind_case();
    f2_kind_case = f2->kind_case();
    f3_kind_case = f3->kind_case();

    if (f1_kind_case != KVListString && f1_kind_case != KVString) {
      return invalid_type_status;
    }
    if (f2_kind_case != KVListString && f2_kind_case != KVString) {
      return invalid_type_status;
    }
    if (f3_kind_case != KVListString && f3_kind_case != KVString) {
      return invalid_type_status;
    }

    std::vector<std::string> feature_values1;
    std::vector<std::string> feature_values2;
    std::vector<std::string> feature_values3;

    if (f1_kind_case == KVString) {
      feature_values1.emplace_back(f1->v_string());
    } else {
      for (int i = 0; i < f1->v_list_string().k_size(); i++) {
        feature_values1.emplace_back(f1->v_list_string().k(i));
      }
    }

    if (f2_kind_case == KVString) {
      feature_values2.emplace_back(f2->v_string());
    } else {
      for (int i = 0; i < f2->v_list_string().k_size(); i++) {
        feature_values2.emplace_back(f2->v_list_string().k(i));
      }
    }

    if (f3_kind_case == KVString) {
      feature_values3.emplace_back(f3->v_string());
    } else {
      for (int i = 0; i < f3->v_list_string().k_size(); i++) {
        feature_values3.emplace_back(f3->v_list_string().k(i));
      }
    }
    feature_proto::Feature feature;
    auto* list_string = feature.mutable_v_list_string();

    for (std::string& v1 : feature_values1) {
      for (std::string& v2 : feature_values2) {
        for (std::string& v3 : feature_values3) {
          list_string->add_k(v1 + kCombineSplitChar + v2 + kCombineSplitChar + v3);
        }
      }
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

// Instant document feature parser
// Doc Comment feaure, will be process in LongDiscrete method
Status MANUAL_METHOD_PARSER_DEFINITION(algo_CommentNum) {
  feature_proto::Feature feature;
  int32_t comment_num = 0;
  if (docinfo->__isset.comment_num) {
    comment_num = docinfo->comment_num;
  }
  feature.set_v_int32(comment_num);
  (*feature_map)[feature_conf.id] = std::move(feature);

  FeatureLog<int32_t>(feature_conf, comment_num);
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(algo_DocRecallOrder) {
  if (docinfo->__isset.rank) {
    feature_proto::Feature feature;
    int32_t order = docinfo->rank;
    feature.set_v_int32(order);
    (*feature_map)[feature_conf.id] = std::move(feature);

    feature_proto::Feature hash_feature;
    std::string order_str = std::to_string(order);
    uint64_t hash_value = MAKE_HASH(order_str);
    hash_feature.set_v_uint64(hash_value);
    (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
    FeatureLog<uint64_t>(feature_conf, hash_value);
  }
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(algo_recall_info) {
  const static std::string kDefaultRecall = "null";
  const static uint64_t kDefaultRecallHash = MAKE_HASH(kDefaultRecall);

  feature_proto::Feature hash_feature;
  uint64_t hash_value = kDefaultRecallHash;
  if (docinfo->__isset.recall) {
    hash_value = MAKE_HASH(docinfo->recall);
  }
  hash_feature.set_v_uint64(hash_value);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  if (generate_textual_feature) {
    feature_proto::Feature feature;
    std::string recall = kDefaultRecall;
    if (docinfo->__isset.recall) {
      recall = docinfo->recall;
    }
    feature.set_v_string(recall);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, recall);
  }
  return Status::OK();
}

// WARNING:时间单位为秒
Status MANUAL_METHOD_PARSER_DEFINITION(HourRangeOfTimestamp) {
  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVInt64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  // milliseconds
  int64_t timestamp = depend_feature->v_int64() * 1000;
  int hour = TimeUtils::GetHour(timestamp);

  const std::vector<int>& hours = feature_conf.int_params;
  const std::vector<std::string>& params = feature_conf.string_params;

  // discrate hour
  int i = 0;
  for (i = 0; i < hours.size(); ++i) {
    if (hour <= hours[i]) {
      break;
    }
  }
  std::string f;
  if (i == 0) {
    f = "_" + params[0];
  } else if (i >= hours.size()) {
    f = params[hours.size() - 1] + "_";
  } else {
    f = params[i - 1] + "_" + params[i];
  }

  if (generate_textual_feature) {
    feature_proto::Feature feature;
    feature.set_v_string(f);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, f);
  }

  feature_proto::Feature hash_feature;
  uint64_t hash_value = MAKE_HASH(f);
  hash_feature.set_v_uint64(hash_value);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  return Status::OK();
}

// WARNING:时间单位为秒
Status MANUAL_METHOD_PARSER_DEFINITION(WeekOfTimestamp) {
  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVInt64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  // milliseconds
  int64_t timestamp = depend_feature->v_int64() * 1000;
  int week = TimeUtils::GetWeekday(timestamp);

  std::string v = std::to_string(week);

  if (generate_textual_feature) {
    feature_proto::Feature feature;
    feature.set_v_string(v);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, v);
  }

  feature_proto::Feature hash_feature;
  uint64_t hash_value = MAKE_HASH(v);
  hash_feature.set_v_uint64(hash_value);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(LongDiscrete) {
  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVUint64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  int32_t comment_num = depend_feature->v_int32();

  const std::vector<int>& int_params = feature_conf.int_params;
  int i = 0;
  for (i = 0; i < int_params.size(); ++i) {
    if (comment_num <= int_params[i]) {
      break;
    }
  }

  const std::vector<uint64_t>& hash_disc_params = feature_conf.hash_disc_params;

  if (generate_textual_feature) {
    const std::vector<std::string>& disc_params = feature_conf.disc_params;
    const std::string& v = disc_params[i];
    feature_proto::Feature feature;
    feature.set_v_string(v);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, v);
  }

  feature_proto::Feature hash_feature;
  uint64_t hash_value = hash_disc_params[i];
  hash_feature.set_v_uint64(hash_value);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(TimeIntervalInHour) {
  Status status = CheckDepends(feature_conf, 2);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f1 = NULL;
  status = GetDependFeature(feature_conf, KVInt64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KVInt64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                            NOT_HASH_FEATURE, f2);
  CHECK_STATUS_RETURN(status);

  if (f1->kind_case() != KVInt64) {
    return invalid_type_status;
  }
  if (f2->kind_case() != KVInt64) {
    return invalid_type_status;
  }

  int64_t interval = f1->v_int64() - f2->v_int64();
  // 单位为秒
  int hour = interval / (60 * 60);

  const std::vector<int>& hours = feature_conf.int_params;

  int i = 0;
  for (i = 0; i < hours.size(); ++i) {
    if (hour <= hours[i]) {
      break;
    }
  }

  if (generate_textual_feature) {
    const std::vector<std::string>& disc_params = feature_conf.disc_params;
    const std::string& v = disc_params[i];
    feature_proto::Feature feature;
    feature.set_v_string(v);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, v);
  }

  const std::vector<uint64_t>& hash_disc_params = feature_conf.hash_disc_params;
  feature_proto::Feature hash_feature;
  uint64_t hash_value = hash_disc_params[i];
  hash_feature.set_v_uint64(hash_value);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  return Status::OK();
}

// click_history
Status MANUAL_METHOD_PARSER_DEFINITION(PreNClickInterval) {
  Status status = CheckDepends(feature_conf, 2);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f1 = NULL;
  status = GetDependFeature(feature_conf, KVInt64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KVListStringInt64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                            NOT_HASH_FEATURE, f2);
  CHECK_STATUS_RETURN(status);

  if (f1->kind_case() != KVInt64 || f2->kind_case() != KVListStringInt64) {
    return invalid_type_status;
  }

  int topn = -1;
  if (!feature_conf.int_params.empty()) {
    topn = feature_conf.int_params[0];
  }
  if (unlikely(topn < 0)) {
    return invalid_param_status;
  }

  const auto& click_history = f2->v_list_string_int64();
  if (topn >= click_history.k_size() || topn >= click_history.w_size()) {
    static const Status not_enough_click_history(error::FEATURE_PARSE_ERROR,
        "not enough click history");
    return not_enough_click_history;
  }

  int64_t timestamp = f1->v_int64();
  // WARN:already sorted in ranker
  uint64_t time_inter_min = (timestamp - click_history.w(topn) / 1000) / 60;

  //算法组定义的时间区间，用一个数组表示,这个是这个数组的长度
  //用start表示向时间区间数组进行插入排序时的当前下标
  // int click_history_tm_map_len = 0, start = 0;
  int i = 0;
  for (i = 0; i < g_click_history_tm_vector.size(); i++) {
    if (time_inter_min <= g_click_history_tm_vector[i]) {
      break;
    }
  }

  if (generate_textual_feature) {
    const std::string& v = g_click_history_tm_str_vector[i];
    feature_proto::Feature feature;
    feature.set_v_string(v);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, v);
  }

  feature_proto::Feature hash_feature;
  hash_feature.set_v_uint64(g_click_history_tm_hash_vector[i]);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, g_click_history_tm_hash_vector[i]);
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(Interest) {
  if (disc_interest_ == nullptr) {
    return Status(error::FEATURE_PARSE_ERROR, "discrate interest is empty");
  }

  feature_proto::Feature* depend_feature = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVListStringDouble, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  int topn = -1;
  if (!feature_conf.int_params.empty()) {
    topn = feature_conf.int_params[0];
  }
  if (unlikely(topn < 0)) {
    return invalid_param_status;
  }

  const auto& manually_category = depend_feature->v_list_string_double();
  if (topn > manually_category.k_size()) {
    topn = manually_category.k_size();
  }

  feature_proto::Feature hash_feature;
  auto* list_hash = hash_feature.mutable_v_list_uint64();

  feature_proto::Feature feature;
  feature_proto::ListString* list_string = NULL;
  if (generate_textual_feature) {
    list_string = feature.mutable_v_list_string();
  }

  // std::unordered_map<std::string, std::vector<double>>::iterator iter;
  std::unordered_map<std::string, DiscInterest>::iterator iter;
  for (int index = 0; index < topn; ++index) {
    iter = disc_interest_->find(manually_category.k(index));
    if (iter != disc_interest_->end()) {
      const DiscInterest& disc = iter->second;
      // discrate level define bash on score * 100
      double score = manually_category.w(index) * 100;
      int i = 0;
      // for (i = 0; i < iter->second.size(); ++i) {
      for (i = 0; i < disc.disc_range.size(); ++i) {
        if (score <= disc.disc_range[i]) {
          break;
        }
      }
      uint64_t hash_value = disc.disc_range_hash[i];
      list_hash->add_k(hash_value);
      FeatureLog<uint64_t>(feature_conf, hash_value);
      if (generate_textual_feature) {
        // list_string->add_k(std::move(v));
        list_string->add_k(disc.disc_range_str[i]);
      }
    }
  }

  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  if (generate_textual_feature) {
    (*feature_map)[feature_conf.id] = std::move(feature);
  }

  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(SubtractCommon) {
  feature_proto::Feature* f1 = NULL;
  Status status;
  status = GetDependFeature(feature_conf, KVListUint64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KVListUint64, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                            HASH_FEATURE, f2);
  // if (!status.ok()) {
  //   LOG(INFO) << "There is no " << feature_conf.name;
  // }

  feature_proto::Feature hash_feature;
  auto* list_hash = hash_feature.mutable_v_list_uint64();

  int topn = -1;
  if (!feature_conf.int_params.empty()) {
    topn = feature_conf.int_params[0];
  }

  if (unlikely(topn <= 0)) {
    return invalid_param_status;
  }

  if (topn > f1->v_list_uint64().k_size()) {
    topn = f1->v_list_uint64().k_size();
  }

  int count = 0;

  std::unordered_set<uint64_t> f2_set;
  if (f2 != NULL) {
    for (int j = 0; j < f2->v_list_uint64().k_size(); j++) {
      f2_set.insert(f2->v_list_uint64().k(j));
    }
  }

  for (int i = 0, count = 0;
      i < f1->v_list_uint64().k_size() && count < topn; ++i) {
    uint64_t v1 = f1->v_list_uint64().k(i);
    if (f2_set.find(v1) == f2_set.end()) {
      list_hash->add_k(v1);
      FeatureLog<uint64_t>(feature_conf, v1);
      ++count;
    } else {
      continue;
    }
  }
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);

  // Generate Textual Feature
  if (generate_textual_feature) {
    status = GetDependFeature(feature_conf, KVListString, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                              NOT_HASH_FEATURE, f1);
    CHECK_STATUS_RETURN(status);

    status = GetDependFeature(feature_conf, KVListString, up_feature_map, cache_up_feature_map,
                              doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                              NOT_HASH_FEATURE, f2);
    if (!status.ok()) {
      LOG(INFO) << "There is no " << feature_conf.name;
    }

    feature_proto::Feature feature;
    auto* list_string = feature.mutable_v_list_string();

    std::unordered_set<std::string> f2_str_set;
    if (f2 != NULL) {
      for (int j = 0; j < f2->v_list_string().k_size(); j++) {
        f2_str_set.insert(f2->v_list_string().k(j));
      }
    }

    for (int i = 0, count = 0;
        i < f1->v_list_string().k_size() && count < topn; ++i) {
      const std::string& v1 = f1->v_list_string().k(i);
      if (f2_str_set.find(v1) == f2_str_set.end()) {
        list_string->add_k(v1);
        FeatureLog<std::string>(feature_conf, v1);
        ++count;
      }
    }
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(ai_commentCount) {
  feature_proto::Feature feature;
  int32_t comment_num = 0;
  if (docinfo->__isset.comment_num) {
    comment_num = docinfo->comment_num;
  }
  feature.set_v_int32(comment_num);
  (*feature_map)[feature_conf.id] = std::move(feature);

  FeatureLog<int32_t>(feature_conf, comment_num);
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(ai_recallInterests) {
  feature_proto::Feature feature;
  std::string recall = "null";
  if (docinfo->__isset.recall) {
    recall = docinfo->recall;
  }
  feature.set_v_string(recall);
  (*feature_map)[feature_conf.id] = std::move(feature);
  FeatureLog<std::string>(feature_conf, recall);
  return Status::OK();
}

Status MANUAL_METHOD_PARSER_DEFINITION(ai_recallRank) {
  if (docinfo->__isset.rank) {
    feature_proto::Feature feature;
    int32_t order = docinfo->rank;
    feature.set_v_int32(order);
    (*feature_map)[feature_conf.id] = std::move(feature);
  }
  return Status::OK();
}

// 物料统计特征处理函数
Status MANUAL_METHOD_PARSER_DEFINITION(NumericDiscrete) {
  if (stat_disc_ == nullptr) {
    return Status(error::FEATURE_PARSE_ERROR, "stat disc is empty");
  }

  const auto& it = stat_disc_->find(feature_conf.name);
  if (it == stat_disc_->end()) {
    return invalid_doc_stat_status;
  }

  Status status;
  feature_proto::Feature* depend_feature = NULL;
  status = GetDependFeature(feature_conf, KVInt32, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, depend_feature);
  CHECK_STATUS_RETURN(status);

  const std::vector<int>& range = it->second.disc_range;
  int64_t v = depend_feature->v_int32();
  int i = 0;
  for (i = 0; i < range.size(); ++i) {
    if (v <= range[i]) {
      break;
    }
  }

  const std::vector<uint64_t>& range_hash = it->second.disc_range_hash;
  feature_proto::Feature hash_feature;
  uint64_t hash_value = range_hash[i];
  hash_feature.set_v_uint64(hash_value);
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  if (generate_textual_feature) {
    const std::vector<std::string>& range_str = it->second.disc_range_str;
    const std::string& feature_value = range_str[i];
    feature_proto::Feature feature;
    feature.set_v_string(feature_value);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, feature_value);
  }

  return Status::OK();
}

// wilson公式计算点击率，保留两位小数, f1=曝光数,f2=点击数
Status MANUAL_METHOD_PARSER_DEFINITION(Divide) {
  Status status;
  feature_proto::Feature* f1 = NULL;
  status = GetDependFeature(feature_conf, KVInt32, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, FIRST_DEPEND,
                            NOT_HASH_FEATURE, f1);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature* f2 = NULL;
  status = GetDependFeature(feature_conf, KVInt32, up_feature_map, cache_up_feature_map,
                            doc_feature_map, instant_feature_map, cache_feature_map, SECOND_DEPEND,
                            NOT_HASH_FEATURE, f2);
  CHECK_STATUS_RETURN(status);

  feature_proto::Feature hash_feature;

  int exp = f1->v_int32();
  int click = f2->v_int32();

  if (exp == 0) {
    return feature_parse_error_status;
  }

  double p_z = 2.0;
  double pos_rat = click * 1.0 / exp * 1.0;

  double score = (pos_rat + (p_z * p_z / (2.0 * exp)) - (
                  (p_z / (2.0 * exp))
                  * sqrt(4.0 * exp * (1.0 - pos_rat) * pos_rat + p_z * p_z))
                 ) / (1.0 + p_z * p_z / exp);

  // 保留两位小数
  score = round(score * 100) / 100.0;
  std::string v = std::to_string(score);
  uint64_t hash_value = MAKE_HASH(v);
  hash_feature.set_v_uint64(MAKE_HASH(v));
  (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  FeatureLog<uint64_t>(feature_conf, hash_value);

  if (generate_textual_feature) {
    feature_proto::Feature feature;
    feature.set_v_string(std::move(v));
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<std::string>(feature_conf, v);
  }

  return Status::OK();
}

}  // namespace feature_engine
