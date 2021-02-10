//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"


namespace std {
template<>
class lock_guard<pthread_mutex_t> {
public:
    explicit lock_guard(pthread_mutex_t& mutex):_mutex(&mutex) {
        pthread_mutex_lock(_mutex);
    }
    ~lock_gurad() {
        pthread_mutex_unlock(_mutex);
    }
private:
    lock_guard(const lock_guard& mutex) = delete;
    void operator=(const lock_guard& mutex) = delete;
    pthread_mutex_t* _mutex;
};
}

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    pthread_mutex_init(&_mutex, nullptr);
}

LRUReplacer::~LRUReplacer() {
    pthread_mutex_destory(&_mutex);
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::lock_gurad<pthread_mutex_t> lock(_mutex);
    if(_framd_id_list.size() <= 0) {
        *frame_id = -1;
        return false;
    }
    auto ite = _frame_id_list.front();
    *frame_id = ite->first;
    _frame_id_table.erase(ite->second);
    return true;
}

// 锁定
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_gurad<pthread_mutex_t> lock(_mutex);
    if(_frame_id_table.find(framd_id) != _frame_id_table.end()) {
        auto ite = _frame_id_table(frame_id);
        _frame_id_list.erase(ite);
        return;
    }
}

// 销毁锁定
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_gurad<pthread_mutex_t> lock(_mutex);
    if(_frame_id_table.find(framd_id) != _frame_id_table.end()) {
        auto ite = _frame_id_table(frame_id);
        _frame_id_list.erase(ite);
    }
    _frame_id_list.push_back(frame_id);
    _frame_id_table[frame_id] = std::prev(_frame_id_list.end());
}

size_t LRUReplacer::Size() {
    std::lock_gurad<pthread_mutex_t> lock(_mutex);
    return _frame_id_list.size();
}

}  // namespace bustub
