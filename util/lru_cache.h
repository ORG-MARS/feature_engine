// File   lru_cache.h
// Author lidongming
// Date   2018-12-13 18:41:30
// Brief

#ifndef FEATURE_ENGINE_UTIL_LRU_CACHE_H_
#define FEATURE_ENGINE_UTIL_LRU_CACHE_H_

#include <list>
#include <vector>
#include <unordered_map>

namespace feature_engine {

template <class K, class T>
struct Node {
    K key;
    T data;
    Node *prev, *next;
};

template <class K, class T>
class LRUCache {
 public:
  LRUCache(size_t size) {
   entries_ = new Node<K, T>[size];
   for(int i = 0; i < size; ++i) {
     free_entries_.push_back(entries_ + i);
   }
   head_ = new Node<K, T>;
   tail_ = new Node<K, T>;
   head_->prev = NULL;
   head_->next = tail_;
   tail_->prev = head_;
   tail_->next = NULL;
  }

  ~LRUCache() {
    delete head_;
    delete tail_;
    delete[] entries_;
  }

  void Put(K key, const T& data) {
    Node<K, T> *node = NULL;
    auto it = hashmap_.find(key);
    if (it != hashmap_.end()) {
      node = it->second;
    }
    if (node) {
      Detach(node);
      node->data = std::move(data);
      Attach(node);
    } else {
      if (free_entries_.empty()) {
        node = tail_->prev;
        Detach(node);
        hashmap_.erase(node->key);
      } else {
        node = free_entries_.back();
        free_entries_.pop_back();
      }
      node->key = key;
      node->data = data;
      hashmap_[key] = node;
      Attach(node);
    }
  }

  int Get(K key, T& data) {
    Node<K, T>* node = NULL;
    auto it = hashmap_.find(key);
    if (it != hashmap_.end()) {
      node = it->second;
    }
    if (node) {
      Detach(node);
      Attach(node);
      data = node->data;
      return 0;
    } else {
      return -1;
    }
  }

  void Foreach() {
    auto p = head_->next;
    while (p && p != tail_) {
      // std::cout << "key=" << p->key << " data=" << p->data << std::endl;
      p = p->next;
    }
  }

  bool Contains(K key) {
    return hashmap_.find(key) != hashmap_.end();
  }

 private:
  void Detach(Node<K, T>* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
  }
  void Attach(Node<K, T>* node) {
    node->prev = head_;
    node->next = head_->next;
    head_->next = node;
    node->next->prev = node;
  }
 private:
  std::unordered_map<K, Node<K, T>* > hashmap_;
  std::list<Node<K, T>* > free_entries_;
  Node<K, T> *head_, *tail_;
  Node<K, T> *entries_;
};

}

#endif
