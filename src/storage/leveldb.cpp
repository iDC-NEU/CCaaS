//
// Created by zwx on 23-6-30.
//

#include "storage/leveldb.h"

namespace Taas {
    Context LevelDB::ctx;
    AtomicCounters_Cache
            LevelDB::epoch_should_push_down_txn_num(10, 1), LevelDB::epoch_pushed_down_txn_num(10, 1);
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  LevelDB::task_queue, LevelDB::redo_log_queue;
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            LevelDB::tikv_epoch_redo_log_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> LevelDB::epoch_redo_log_complete;
}
