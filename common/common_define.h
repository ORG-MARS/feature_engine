// File   common_define.h
// Author lidongming
// Date   2018-09-05 14:06:37
// Brief

#ifndef FEATURE_ENGINE_COMMON_COMMON_DEFINE_H_
#define FEATURE_ENGINE_COMMON_COMMON_DEFINE_H_

#include "feature_engine/deps/cityhash/include/city.h"

namespace feature_engine {

// uint64_t
static inline uint64_t MAKE_HASH(char* buf) {
  return CityHash64(buf, strlen(buf));
}
static inline uint64_t MAKE_HASH(const std::string& str) {
  return CityHash64(str.c_str(), str.size());
}

static inline std::string random_string(size_t length) {
  auto randchar = []() -> char
  {
          const char charset[] =
          "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
          const size_t max_index = (sizeof(charset) - 1);
          return charset[rand() % max_index];
  };
  std::string str(length,0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

// #define MAKE_HASH(str) CityHash64(str.c_str(), str.size())
#define GEN_HASH2(h1, h2) (((h1) << 1) ^ (h2))
#define GEN_HASH3(h1, h2, h3) (((GEN_HASH2(h1, h2)) << 1) ^ (h3))

// 分支预测
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#ifdef __GNUC__
#define NOT_USED __attribute__ ((unused))
#else
#define NOT_USED
#endif

// get array size
#define GET_ARRAY_LEN(array, len) { len = (sizeof(array) / sizeof(array[0])); } 

// Utilities
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&); \
    void operator=(TypeName&)
#endif

#ifndef DISALLOW_IMPLICIT_CONSTRUCTORS
#define DISALLOW_IMPLICIT_CONSTRUCTORS(TypeName) \
    TypeName();                                  \
    DISALLOW_COPY_AND_ASSIGN(TypeName);
#endif

#if 0
// Brief: Bind Static Functions
// Usage: BindStaticFunction<int, int, int, int> handler(sum);  
//        std::cout << "handler(1, 2, 3) : " << handler(1, 2, 3) << std::endl; 
template<typename R = int, typename... Args>
class BindStaticFunction {
 public:
  BindStaticFunction(std::function<R(Args...)> handler)
      : handler_(handler) { }
  R operator() (Args... args) { return handler_(args...); } 
 private:  
  std::function<R(Args...) > handler_;  
};

// Brief: Bind Member Functions
// Usage: MyClass* my_class_ = new MyClass();
//        BindMemberFunction<MyClass, int, int> binder(my_class_, Foo);
//        int a, b;
//        binder(a, b); // Equals to my_class_->Foo(a, b);
template<typename Object, typename R = int, typename... Args>
class BindMemberFunction {
 public:
  BindMemberFunction(Object* object, R (Object::*method)(Args...))
      : object_(object) {
      handler_ = [object,method](Args... args) {
          return (*object.*method)(args...);
      };
  }
  R operator() (Args... args) { return handler_(args...); } 
 private:  
  Object* object_;
  std::function<R(Args...) > handler_;  
};
#endif

}  // namespace feature_engine

#endif  // FEATURE_ENGINE_COMMON_COMMON_DEFINE_H_
