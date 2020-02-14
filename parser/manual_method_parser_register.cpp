// File   manual_method_parser_register.cpp
// Author lidongming
// Date   2018-09-11 00:40:56
// Brief

#include "feature_engine/parser/manual_method_parser.h"
#include "feature_engine/deps/glog/include/glog/logging.h"

namespace feature_engine {

using namespace commonlib;

#define REGISTER_PARSER(_parser_) \
  RegisterParser(#_parser_, _parser_);

void ManualMethodParser::RegisterParser(std::string parser_name,
    ManualMethodParserFunc parser) {
  parser_map_[parser_name] = std::move(parser);
}
void ManualMethodParser::Register() {
  REGISTER_PARSER(Simple);
  REGISTER_PARSER(MultiKV1TopN);
  REGISTER_PARSER(MultiKV2TopN);
  REGISTER_PARSER(SegmentTopN);
  REGISTER_PARSER(MatchTopic1);
  REGISTER_PARSER(MatchTopic2);
  REGISTER_PARSER(MatchTopic3);
  REGISTER_PARSER(MatchLength1);
  REGISTER_PARSER(MatchLength2);
  REGISTER_PARSER(MatchLength3);
  REGISTER_PARSER(Combine2);
  REGISTER_PARSER(Combine3);
  REGISTER_PARSER(MultiLevelDiscrete);
  REGISTER_PARSER(IntDiscrete);

  REGISTER_PARSER(HourRangeOfTimestamp);
  REGISTER_PARSER(WeekOfTimestamp);
  REGISTER_PARSER(LongDiscrete);
  REGISTER_PARSER(TimeIntervalInHour);
  REGISTER_PARSER(PreNClickInterval);

  REGISTER_PARSER(SubtractCommon);
  REGISTER_PARSER(Interest);
  REGISTER_PARSER(NumericDiscrete);
  REGISTER_PARSER(Divide);

  // Instant document feature parser
  REGISTER_PARSER(algo_CommentNum);
  REGISTER_PARSER(algo_recall_info);
  REGISTER_PARSER(algo_DocRecallOrder);
  REGISTER_PARSER(ai_commentCount);
  REGISTER_PARSER(ai_recallInterests);
  REGISTER_PARSER(ai_recallRank);

  REGISTER_PARSER(algo_doc_exp_num);
  REGISTER_PARSER(algo_doc_click_num);
  REGISTER_PARSER(algo_doc_read_num);
  REGISTER_PARSER(algo_doc_read_duration);
  REGISTER_PARSER(algo_doc_read_progress);
  REGISTER_PARSER(algo_doc_avg_duration);
  REGISTER_PARSER(algo_doc_avg_progress);
  REGISTER_PARSER(algo_doc_ding_num);
  REGISTER_PARSER(algo_doc_post_num);
}

}
