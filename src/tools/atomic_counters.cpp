//
// Created by 周慰星 on 11/8/22.
//

#include <cassert>
#include "tools/atomic_counters.h"

namespace Taas{
    AtomicCounters::AtomicCounters(uint64_t size){
        _size = size;
        for(int i = 0; i < (int)size; i ++) {
            vec.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        }
    }

    void AtomicCounters::Init(uint64_t size){
        if(size < _size) return;
        _size = size;
        vec.resize(size);
        for(int i = 0; i < (int)size; i ++) {
            vec[i] = std::make_unique<std::atomic<uint64_t>>(0);
        }
    }

}