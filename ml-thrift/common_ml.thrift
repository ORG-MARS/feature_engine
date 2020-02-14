namespace cpp common_ml_thrift

// comes from common.thrift
enum LABEL_TYPE {
  W2V_CLUSTERING  = 2       // word2vec 模型
  MANUALLY_CATEGORY = 18    // 编辑定义的分类
  MANUALLY_INTEREST = 20    // 编辑定义的兴趣点
  MANUALLY_INTEREST_NEG = 0x10014 //负反馈兴趣点
  VIDEO_W2V_CLUSTERING = 0x30002  // 视频频道word2vec模型
  VIDEO_MANUALLY_CATEGORY = 0x30012 //视频频道编辑定义的分类
  VIDEO_MANUALLY_INTEREST = 0x30014 //视频频道编辑定义的兴趣点
  VIDEO_MANUALLY_INTEREST_NEG = 0x40014 //视频兴趣点负反馈
  VIDEO_TITLE_KW = 0x30006  //视频频道标题关键词
  VIDEO_MANUALLY_KW = 0x3000C  //视频频道编辑定义关键词
}

struct UserProfile {
  1: optional string key;
  2: optional double score;
  3: optional i32 type;
}

struct DocAndTs {
  1: optional string element;  // docid
  2: optional i64 score;       // timestamp(milliseconds)
}

struct UserInfo {
  1: required string uid;
  2: required string device_id;
  3: optional i16 age;
  4: optional string gender;
  5: optional string location_code;
  6: optional list<UserProfile> user_longterm_profile;
  7: optional list<UserProfile> user_realtime_profile;
  8: optional list<DocAndTs> click_doc;
  9: optional list<DocAndTs> click_video;
  10: optional string canal;
  11: optional list<string> occupation_info;
  12: optional map<string, string> user_stats;  // 用户统计相关的特征，比如曝光次数、点击次数等

  21: optional string tid;
  22: optional string tid_desc;
  27: optional i32 fresh_num;
  26: optional i64 timestamp;   // 单位为秒
  23: optional string nettype;
  24: optional string device_vendor;
  25: optional string device_model;
  28: optional string device_brand;
}

struct DocInfo {
  1: required string docid;
  2: optional i32 comment_num;
  3: optional i32 recall_info;  // NOT USED, use recall instead
  4: optional string recall;
  5: optional i32 rank;
}
