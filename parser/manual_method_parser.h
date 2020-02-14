// File   method_lib.h
// Author lidongming
// Date   2018-09-10 23:30:47
// Brief

#ifndef FEATURE_ENGINE_PARSER_MANUAL_METHOD_PARSER_H_
#define FEATURE_ENGINE_PARSER_MANUAL_METHOD_PARSER_H_

#include <string>
#include <map>
#include "feature_engine/deps/commonlib/include/status.h"
#include "feature_engine/ml-thrift/gen-cpp/feature_types.h"
#include "feature_engine/proto/feature.pb.h"
#include "feature_engine/parser/parser.h"
#include "feature_engine/parser/parser_common.h"
#include "feature_engine/feature/feature_define.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/parser/parser_common.h"
#include "feature_engine/index/document.h"
#include "feature_engine/reloader/data_manager.h"

namespace feature_engine {

using commonlib::Status;
using reloader::StatDisc;
using reloader::DiscInterest;
 
using ManualMethodParserFunc
    = std::function<Status(bool generate_textual_feature,
        const FeatureConf& feature_conf, const DocStat& doc_stat,
        FeatureMap* up_feature_map, FeatureMap* cache_up_feature_map,
        FeatureMap* doc_feature_map, FeatureMap* instant_feature_map,
        FeatureMap* cache_instant_feature_map,
        const common_ml_thrift::DocInfo* docinfo,
        FeatureMap* context_feature_map)>;

#define DECLARE_METHOD_PARSER(_method_name_) \
static Status _method_name_( \
      bool generate_textual_feature, const FeatureConf& feature_conf, \
      const DocStat& doc_stat, \
      FeatureMap* up_feature_map, FeatureMap* cache_up_feature_map, \
      FeatureMap* doc_feature_map, FeatureMap* instant_feature_map, \
      FeatureMap* cache_instant_feature_map, \
      const common_ml_thrift::DocInfo* docinfo, \
      FeatureMap* feature_map)

#define DEFINE_INLINE_METHOD_PARSER(_method_name_) \
static inline Status _method_name_( \
      bool generate_textual_feature, const FeatureConf& feature_conf, \
      const DocStat& doc_stat, \
      FeatureMap* up_feature_map, FeatureMap* cache_up_feature_map, \
      FeatureMap* doc_feature_map, FeatureMap* instant_feature_map, \
      FeatureMap* cache_instant_feature_map, \
      const common_ml_thrift::DocInfo* docinfo, \
      FeatureMap* feature_map)

class ManualMethodParser : public Parser {
 public:
  // -------------------------------Method Parsers------------------------------
  ManualMethodParser() {
    reloader::DataManager& data_manager = reloader::DataManager::Instance();
    disc_interest_ = data_manager.GetDiscInterestDB();
    stat_disc_ = data_manager.GetStatDiscDB();
  }

  void parsers(std::vector<std::string>* parsers) const override {
    // FIXME(lidongming):IMPLEMENT
  }
  void ReloadParsers() override {
    // FIXME(lidongming):IMPLEMENT
  }

  void Register() override;

  std::map<std::string, ManualMethodParserFunc>& parser_map() {
    return parser_map_;
  }

  void RegisterParser(std::string parser_name, ManualMethodParserFunc parser);

  static inline Status GetDependFeature(const FeatureConf& feature_conf,
      const feature_proto::Feature::KindCase kind_case,
      FeatureMap* up_feature_map,       // 用户特征
      FeatureMap* cache_up_feature_map, // 缓存的用户特征
      FeatureMap* doc_feature_map,      // 基础物料特征
      FeatureMap* instant_feature_map,  // 实时特征
      FeatureMap* cache_feature_map,    // 缓存的物料特征
      int depend_seq, bool get_hash_feature,
      feature_proto::Feature*& depend_feature) {
    if (unlikely(feature_conf.depends.empty())) {
      return Status(commonlib::error::FEATURE_PARSE_ERROR, "depends is empty");
    }

    if (depend_seq >= feature_conf.depends.size()) {
      return Status(commonlib::error::FEATURE_PARSE_ERROR,
                    "feature seq out of range");
    }

    int depend_feature_id = feature_conf.depends[depend_seq];
    FeatureConf* depend_feature_conf
      = FeatureConfParser::GetInstance().GetFeatureConf(depend_feature_id);

    if (unlikely(depend_feature_conf == NULL)) {
      return depend_not_found_status;
    }

    FeatureMap* depend_feature_map = NULL;
    if (depend_feature_conf->source == USER_PROFILE) {
      if (depend_feature_conf->enable_cache && cache_up_feature_map) {
        depend_feature_map = cache_up_feature_map;
      } else {
        depend_feature_map = up_feature_map;
      }
    } else if (depend_feature_conf->source == DOC_PROFILE) {
      if (depend_feature_conf->enable_cache && cache_feature_map) {
        depend_feature_map = cache_feature_map;
      } else {
        if (depend_feature_conf->instant) {
          depend_feature_map = instant_feature_map;
        } else {
          depend_feature_map = doc_feature_map;
        }
      }
    } else {
      depend_feature_map = instant_feature_map;
    }

    if (unlikely(depend_feature_map == NULL)) {
      return Status(commonlib::error::FEATURE_PARSE_ERROR,
                    "feature map is null");
    }

    if (get_hash_feature) {
      depend_feature_id += kHashFeatureOffset;
    }

    feature_proto::Feature* feature = NULL;
    auto it = depend_feature_map->find(depend_feature_id);
    if (it != depend_feature_map->end()) {
      feature = &it->second;
    }

    if (feature == NULL) {
#ifndef NDEBUG
      LOG(INFO) << "depend feature not found feature_name:" << feature_conf.name
        << " depend_feature_id:" << feature_conf.depends[depend_seq]
        << " use_feature_id:" << depend_feature_id
        << " offset:" << kHashFeatureOffset;
#endif
      return depend_not_found_status;
    } else {
      depend_feature = feature;
    }

    return CheckFeatureType(*depend_feature, kind_case);
  }

  // Default feature for 'MatchLength*'
  static inline void MakeZeroMatchLengthFeatures(const FeatureConf& feature_conf,
      FeatureMap* feature_map) {
    feature_proto::Feature feature;
    feature_proto::Feature hash_feature;
    static const std::string zero_match_count = "0";
    static const uint64_t zero_match_count_hash = MAKE_HASH(zero_match_count);
    feature.set_v_string(zero_match_count);
    hash_feature.set_v_uint64(zero_match_count_hash);
    (*feature_map)[feature_conf.id] = std::move(feature);
    (*feature_map)[HASH_FEATURE_ID(feature_conf.id)] = std::move(hash_feature);
  }

  static Status Match(bool generate_textual_feature,
                      bool match_length, const FeatureConf& feature_conf,
                      FeatureMap* up_feature_map, FeatureMap* cache_up_feature_map,
                      FeatureMap* doc_feature_map, FeatureMap* instant_feature_map,
                      FeatureMap* cache_feature_map, FeatureMap* feature_map);

  static Status TopN(bool generate_textual_feature,
                     const FeatureConf& feature_conf,
                     FeatureMap* up_feature_map, FeatureMap* cache_up_feature_map,
                     FeatureMap* doc_feature_map, FeatureMap* instant_feature_map,
                     FeatureMap* cache_feature_map, FeatureMap* feature_map);

 private:
  std::map<std::string, ManualMethodParserFunc> parser_map_;

  static std::shared_ptr<
    std::unordered_map<std::string, DiscInterest>> disc_interest_;
  static std::shared_ptr<
    std::unordered_map<std::string, StatDisc>> stat_disc_;

 public:
  DECLARE_METHOD_PARSER(Simple);
  DECLARE_METHOD_PARSER(MultiKV1TopN);
  DECLARE_METHOD_PARSER(MultiKV2TopN);
  DECLARE_METHOD_PARSER(SegmentTopN);
  DECLARE_METHOD_PARSER(MatchTopic1);
  DECLARE_METHOD_PARSER(MatchTopic2);
  DECLARE_METHOD_PARSER(MatchTopic3);
  DECLARE_METHOD_PARSER(MatchLength1);
  DECLARE_METHOD_PARSER(MatchLength2);
  DECLARE_METHOD_PARSER(MatchLength3);
  DECLARE_METHOD_PARSER(Combine2);
  DECLARE_METHOD_PARSER(Combine3);
  DECLARE_METHOD_PARSER(MultiLevelDiscrete);
  DECLARE_METHOD_PARSER(IntDiscrete);

  DECLARE_METHOD_PARSER(HourRangeOfTimestamp);
  DECLARE_METHOD_PARSER(WeekOfTimestamp);
  DECLARE_METHOD_PARSER(LongDiscrete);
  DECLARE_METHOD_PARSER(TimeIntervalInHour);
  DECLARE_METHOD_PARSER(PreNClickInterval);

  // Instant document feature parser
  DECLARE_METHOD_PARSER(algo_CommentNum);
  DECLARE_METHOD_PARSER(algo_recall_info);
  DECLARE_METHOD_PARSER(algo_DocRecallOrder);
  DECLARE_METHOD_PARSER(ai_commentCount);
  DECLARE_METHOD_PARSER(ai_recallInterests);
  DECLARE_METHOD_PARSER(ai_recallRank);

  static inline Status DocDocStatFeature(const FeatureConf& feature_conf,
      const DocStat& doc_stat, FeatureMap* feature_map) {
    if (!doc_stat.valid) {
      return Status(commonlib::error::FEATURE_PARSE_ERROR,
                    "doc_stat is invalid");
    }
    int value = 0;
    if (doc_stat.get_value(feature_conf.name, &value) != 0) {
      return not_found_doc_disc_item_status;
    }
  
    feature_proto::Feature feature;
    feature.set_v_int32(value);
    (*feature_map)[feature_conf.id] = std::move(feature);
    FeatureLog<int32_t>(feature_conf, value);
    return Status::OK();
  }

  DEFINE_INLINE_METHOD_PARSER(algo_doc_exp_num) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_click_num) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_read_num) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_read_duration) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_read_progress) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_avg_duration) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_avg_progress) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_ding_num) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }
  DEFINE_INLINE_METHOD_PARSER(algo_doc_post_num) {
    return DocDocStatFeature(feature_conf, doc_stat, feature_map);
  }

  DECLARE_METHOD_PARSER(SubtractCommon);
  DECLARE_METHOD_PARSER(Interest);
  DECLARE_METHOD_PARSER(NumericDiscrete);
  DECLARE_METHOD_PARSER(Divide);

};  // MethodLib

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_PARSER_MANUAL_METHOD_PARSER_H_
