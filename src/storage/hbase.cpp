//
// Created by user on 23-6-30.
//

#include "storage/hbase.h"

namespace Taas {
    Context Hbase::ctx;
    AtomicCounters_Cache
            Hbase::epoch_should_push_down_txn_num(10, 1), Hbase::epoch_pushed_down_txn_num(10, 1);
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  Hbase::task_queue, Hbase::redo_log_queue;
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            Hbase::tikv_epoch_redo_log_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> Hbase::epoch_redo_log_complete;
}
