#include "feature_engine/deps/gtest/include/gtest/gtest.h"
#include "feature_engine/deps/commonlib/include/time_utils.h"
#include <iostream>

// using namespace feature_engine;
using namespace commonlib;

class TimeUtilsTest : public ::testing::Test {
 public:
  void SetUp() { }
  void TearDown() { }
};

TEST_F(TimeUtilsTest, TestGetHour) {
  TimeUtils::setJetLag();
  int64_t tm = 1543921718000;
  int hour = TimeUtils::GetHour(tm);
  std::cout << "hour:" << hour << std::endl;
  int week = TimeUtils::GetWeekday(tm);
  std::cout << "week:" << week << std::endl;
  // EXPECT_EQ(parser.parse_status(), Status::OK());
}
