//
// Created by user on 24-3-16.
//

#ifndef TAAS_STORAGE_THREAD_COUNTS_H
#define TAAS_STORAGE_THREAD_COUNTS_H

#pragma once

#include "tools/context.h"
#include "tools/atomic_counters.h"
#include "tools/atomic_counters_cache.h"

namespace Taas {
    class storage_thread_counts {
    public:
        uint64_t thread_id = 0, max_length = 0, sharding_num = 0, local_server_id;
        std::atomic<uint64_t> inc_id;

        std::shared_ptr<AtomicCounters_Cache>
                epoch_should_push_down_txn_num_local,
                epoch_pushed_down_txn_num_local;

        std::vector<std::shared_ptr<AtomicCounters_Cache>>
                epoch_should_push_down_txn_num_local_vec,
                epoch_pushed_down_txn_num_local_vec;

    };
}



#endif //TAAS_STORAGE_THREAD_COUNTS_H
