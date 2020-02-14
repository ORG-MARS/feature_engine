// File   feature_conf_parser_test.cpp
// Author lidongming
// Date   2018-09-07 11:53:44
// Brief

#include "feature_engine/deps/gtest/include/gtest/gtest.h"
#include "feature_engine/feature/feature_conf_parser.h"
#include "feature_engine/feature/feature_conf.h"
#include "feature_engine/common/common_gflags.h"

using namespace feature_engine;

class FeatureConfParserTest : public ::testing::Test {
 public:
  void SetUp() { }
  void TearDown() { }
};  // FeatureConfParserTest

TEST_F(FeatureConfParserTest, TestParse) {
  FLAGS_feature_conf_path = "../conf/features";
  FeatureConfParser& parser = FeatureConfParser::GetInstance();
  EXPECT_EQ(parser.parse_status(), Status::OK());
}
