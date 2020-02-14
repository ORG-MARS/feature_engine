// File   data_manager.h
// Author lidongming
// Date   2018-10-17 13:37:21
// Brief

#ifndef RELOADER_RELOADER_DATA_MANAGER_H_
#define RELOADER_RELOADER_DATA_MANAGER_H_

#include <chrono>
#include <thread>
#include <unordered_set>
#include "feature_engine/reloader/reloader.h"
#include "feature_engine/common/common_gflags.h"
#include "feature_engine/common/common_define.h"

namespace feature_engine {
namespace reloader {
 
template<typename Entry, class... Args>
class ReloaderDetachThread {
 public:
   ReloaderDetachThread(ReloaderMeta reloader_meta)
       : reloader_meta_(reloader_meta) {
     entry_ = std::make_shared<Entry>(reloader_meta); 
   }

   void Start() {
     entry_->Load(reloader_meta_);
     LOG(INFO) << "load finished type:" << reloader_meta_.type
               << " file:" << reloader_meta_.file_name;


     std::thread([this]() {
       while (true) {
         entry_->Reload(reloader_meta_);
         std::this_thread::sleep_for(
             std::chrono::seconds(reloader_meta_.interval));
       }
     }).detach();
  }

   std::shared_ptr<Entry> GetEntry() {
     return entry_;
   }

 private:
   std::shared_ptr<Entry> entry_;
   ReloaderMeta reloader_meta_;
};

// 统计特征对应的离散区间，包括用户统计特征离散区间及物料统计特征离散区间
struct StatDisc {
  // 离散区间，eg:[-INT_MIN, 100, 500, 600,...]
  std::vector<int> disc_range;
  // 区间分段字符串, 用于避免特征抽取时频繁重复拼接字符串, eg:["_0", "_1",...]
  std::vector<std::string> disc_range_str;
  // 区间分段字符串对应的hash, 用于加速特征抽取，避免重复hash
  std::vector<uint64_t> disc_range_hash;
};

template<class K, class V>
class StatParser {
 public:
  StatParser() : _sep(",") { }
  explicit StatParser(std::string sep) : _sep(sep) { }

  ParserStatus Parse(std::vector<std::string>& line_vec, K& k, V& v);

 private:
  std::string _sep;
};

// 统计特征Parser类，解析生成StatDisc
template<>
class StatParser<std::string, StatDisc> {
 public:
  StatParser() : _sep(",") { }
  explicit StatParser(std::string sep) : _sep(sep) { }
  ParserStatus Parse(std::vector<std::string>& line_vec, std::string& k,
                     StatDisc& v) {
    if (line_vec.size() < 2) {
      return PARSER_INVALID_LINE;
    }
    k = line_vec[0];
  
    std::vector<std::string> val_vec;
    commonlib::StringUtils::Split(line_vec[1], _sep, val_vec);
    for (const std::string& val : val_vec) {
      v.disc_range.push_back(atoi(val.c_str()));
    }
  
    // eg:[INT_MIN, 10, 20, 30, 40,...]->[_0, _1, _2, _3, _4,...]
    for (int i = 0; i < v.disc_range.size(); i++) {
      std::string range = "_" + std::to_string(i);
      v.disc_range_str.emplace_back(range);
      v.disc_range_hash.push_back(MAKE_HASH(range));
    }

    return PARSER_OK;
  }

 private:
  std::string _sep;
};

struct DiscInterest {
  // 娱乐:-2147483648,0,99,105,387,794,1699,4005,6814,9996,2147483647 
  std::vector<double> disc_range;
  // eg:["娱乐_0", "娱乐_1",...]
  std::vector<std::string> disc_range_str;
  std::vector<uint64_t> disc_range_hash;
};

template<class K, class V>
class DiscParser {
 public:
  DiscParser() : _sep(",") { }
  explicit DiscParser(std::string sep) : _sep(sep) { }

  ParserStatus Parse(std::vector<std::string>& line_vec, K& k, V& v);

 private:
  std::string _sep;
};

template<>
class DiscParser<std::string, DiscInterest> {
 public:
  DiscParser() : _sep(",") { }
  explicit DiscParser(std::string sep) : _sep(sep) { }
  ParserStatus Parse(std::vector<std::string>& line_vec, std::string& k,
                     DiscInterest& v) {
    if (line_vec.size() < 2) {
      return PARSER_INVALID_LINE;
    }
    k = line_vec[0];
  
    std::vector<std::string> val_vec;
    commonlib::StringUtils::Split(line_vec[1], _sep, val_vec);
    for (const std::string& val : val_vec) {
      v.disc_range.push_back(atof(val.c_str()));
    }
  
    for (int i = 0; i < v.disc_range.size(); i++) {
      std::string range = k + "_" + std::to_string(i);
      v.disc_range_str.emplace_back(range);
      v.disc_range_hash.push_back(MAKE_HASH(range));
    }

    return PARSER_OK;
  }

 private:
  std::string _sep;
};

class DataManager {
 public:
  static DataManager& Instance() {
    static DataManager instance;
    return instance;
  }

  using DocReloaderEntry = ReloaderEntry<SimpleSetReloader<std::string,
                               SimpleParser<std::string>>, ReloaderMeta>; 
  using DocReloaderThread = ReloaderDetachThread<DocReloaderEntry, ReloaderMeta>;

  // using DiscReloaderEntry = ReloaderEntry<MapVectorRedisReloader<std::string, double,
  //                              MultiValueParser<std::string, double>>, ReloaderMeta>;
  // using DiscReloaderThread = ReloaderDetachThread<DiscReloaderEntry, ReloaderMeta>;

  using DiscReloaderEntry = ReloaderEntry<MapRedisReloader<
                                std::string, DiscInterest,
                                DiscParser<std::string, DiscInterest>>,
                              ReloaderMeta>;
  using DiscReloaderThread = ReloaderDetachThread<DiscReloaderEntry, ReloaderMeta>;

  // 统计特征Entry及Reloader，用于从redis自动加载数据并解析生成统计特征结构
  using StatReloaderEntry = ReloaderEntry<MapRedisReloader<
                                std::string, StatDisc,
                                StatParser<std::string, StatDisc>>,
                              ReloaderMeta>;
  using StatReloaderThread = ReloaderDetachThread<StatReloaderEntry, ReloaderMeta>;

  void Init() {
    // Reload reserved docs for local file
    ReloaderMeta reloader_meta;
    reloader_meta.type = STATE_CHECKER_FILE;
    reloader_meta.file_name = "./data/reserved_docs.dat";
    reloader_meta.interval = FLAGS_reload_interval;
    doc_reloader_thread_ = std::make_shared<DocReloaderThread>(reloader_meta);
    doc_reloader_thread_->Start();

    // Reload discrete range
    ReloaderMeta disc_interest_reloader_meta;
    disc_interest_reloader_meta.type = STATE_CHECKER_REDIS;
    disc_interest_reloader_meta.key_seprator = ':';
    disc_interest_reloader_meta.value_seprator = ',';
    disc_interest_reloader_meta.redis_host = FLAGS_disc_interest_redis_host;
    disc_interest_reloader_meta.redis_key = FLAGS_disc_interest_redis_key;
    disc_interest_reloader_meta.redis_tm_key = FLAGS_disc_interest_redis_tm_key;
    disc_interest_reloader_meta.interval = FLAGS_reload_interval;
    disc_interest_reloader_thread_
      = std::make_shared<DiscReloaderThread>(disc_interest_reloader_meta);
    disc_interest_reloader_thread_->Start();

    // Reload stat discrete range
    ReloaderMeta stat_reloader_meta;
    stat_reloader_meta.type = STATE_CHECKER_REDIS;
    stat_reloader_meta.key_seprator = ':';
    stat_reloader_meta.value_seprator = ',';
    stat_reloader_meta.redis_host = FLAGS_stat_disc_redis_host;
    stat_reloader_meta.redis_key = FLAGS_stat_disc_redis_key;
    stat_reloader_meta.redis_tm_key = FLAGS_stat_disc_redis_tm_key;
    stat_reloader_meta.interval = FLAGS_reload_interval;
    stat_reloader_thread_ = std::make_shared<StatReloaderThread>(stat_reloader_meta);
    stat_reloader_thread_->Start();

    check();
  }

  std::shared_ptr<std::unordered_set<std::string>> GetDocDB() {
    auto entry = doc_reloader_thread_->GetEntry();
    if (entry == nullptr) {
      // LOG(ERROR) << "doc entry is null";
      return nullptr;
    }
    auto reloader = entry->GetContent();
    if (reloader == NULL) {
      // LOG(ERROR) << "doc reloader is null";
      return nullptr;
    }
    auto db = reloader->GetDB();
    if (db == nullptr) {
      LOG(ERROR) << "doc db is null";
      return nullptr;
    }
    return db;
  }

  // std::shared_ptr<std::unordered_map<std::string, std::vector<double>>>
  std::shared_ptr<std::unordered_map<std::string, DiscInterest>>
   GetDiscInterestDB() {
    auto entry = disc_interest_reloader_thread_->GetEntry();
    if (entry == nullptr) {
      LOG(ERROR) << "disc entry is null";
      return nullptr;
    }
    auto reloader = entry->GetContent();
    if (reloader == NULL) {
      LOG(ERROR) << "disc reloader is null";
      return nullptr;
    }
    auto db = reloader->GetDB();
    if (db == nullptr) {
      LOG(ERROR) << "disc db is null";
      return nullptr;
    }
    return db;
  }

  std::shared_ptr<std::unordered_map<std::string, StatDisc>>
  GetStatDiscDB() {
    auto entry = stat_reloader_thread_->GetEntry();
    if (entry == nullptr) {
      LOG(ERROR) << "stat disc entry is null";
      return nullptr;
    }
    auto reloader = entry->GetContent();
    if (reloader == NULL) {
      LOG(ERROR) << "stat disc reloader is null";
      return nullptr;
    }
    auto db = reloader->GetDB();
    if (db == nullptr) {
      LOG(ERROR) << "stat disc db is null";
      return nullptr;
    }
    return db;
  }

  bool check() {
    std::shared_ptr<std::unordered_map<std::string, DiscInterest>>
      disc_interest = GetDiscInterestDB();
    if (disc_interest == nullptr || disc_interest->empty()) {
      LOG(WARNING) << "invalid disc_interest";
      return false;
    }
    for (const auto& kv : *disc_interest) {
      LOG(INFO) << "[disc_interest] key:" << kv.first;
      const DiscInterest& disc = kv.second;
      for (const int v : disc.disc_range) {
        LOG(INFO) << "[disc_interest]   value:" << v;
      }
    }

    std::shared_ptr<std::unordered_map<std::string, StatDisc>>
      stat_disc = GetStatDiscDB();
    if (stat_disc == nullptr || stat_disc->empty()) {
      LOG(WARNING) << "invalid stat_disc";
      return false;
    }
    for (const auto& kv : *stat_disc) {
      LOG(INFO) << "[stat_disc] key:" << kv.first;
      const StatDisc& stat_disc = kv.second;
      for (const int v : stat_disc.disc_range) {
        LOG(INFO) << "[stat_disc]   value:" << v;
      }
    }
    return true;
  }

 private:
  std::shared_ptr<DocReloaderThread> doc_reloader_thread_;
  std::shared_ptr<DiscReloaderThread> disc_interest_reloader_thread_;
  std::shared_ptr<StatReloaderThread> stat_reloader_thread_;
};  // DataManager

}  // namespace reloader
}  // namespace feature_engine

#endif  // RELOADER_RELOADER_DATA_MANAGER_H_
