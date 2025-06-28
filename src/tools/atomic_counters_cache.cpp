//
// Created by zwx on 24-3-14.
//

#include <cassert>
#include "tools/atomic_counters_cache.h"

namespace Taas{
    AtomicCounters_Cache::AtomicCounters_Cache(uint64_t length, uint64_t size){
        _size = size;
        _length = length;
        vec.resize(length);
        for(int i = 0; i < (int)length; i ++) {
            vec[i] = std::make_unique<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>();
            auto &v = (*vec[i]);
            v.resize(size);
            for(uint64_t j = 0; j < size; j ++) {
                v[j] = std::make_unique<std::atomic<uint64_t>>(0);
            }
        }
    }

    void AtomicCounters_Cache::Init(uint64_t length, uint64_t size, uint64_t value) {
        if(size < _size && length < _length) return ;
        _size = size;
        _length = length;
        vec.resize(length);
        for(unsigned int i = 0; i < length; i ++) {
            vec[i] = std::make_unique<std::vector<std::unique_ptr<std::atomic<uint64_t>>>>();
            auto p = &(*vec[i]);
            assert((uint64_t)p != 0x1);
            auto &v = (*(vec[i]));
            v.resize(size);
            for(unsigned int j = 0; j < size; j ++) {
                v[j] = std::make_unique<std::atomic<uint64_t>>(value);
            }
        }
    }

}
