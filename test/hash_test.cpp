#include "feature_engine/deps/gtest/include/gtest/gtest.h"
#include "feature_engine/common/common_define.h"
#include <iostream>

using namespace feature_engine;

class HashTest : public ::testing::Test {
 public:
  void SetUp() { }
  void TearDown() { }
};

TEST_F(HashTest, TestHash) {
  std::string v0 = "297";
  std::string v1 = "2";
  uint64_t h0 = MAKE_HASH(v0);
  uint64_t h1 = MAKE_HASH(v1);
  uint64_t h = GEN_HASH2(h0, h1);
  std::cout << h << std::endl;
  std::cout << int64_t(h) << std::endl;
  // EXPECT_EQ(parser.parse_status(), Status::OK());
}
