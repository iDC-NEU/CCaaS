//
// Created by user on 24-3-15.
//

#ifndef TAAS_TRANSACTION_CACHE_H
#define TAAS_TRANSACTION_CACHE_H

#pragma once

#include "queue"

#include "zmq.hpp"
#include "proto/transaction.pb.h"

#include "tools/utilities.h"
#include "tools/concurrent_hash_map.h"
#include "epoch/epoch_manager.h"

namespace Taas{
    class TransactionCache {
    public:
        ///message handler
        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
        epoch_backup_txn,
        epoch_insert_set,
        epoch_abort_set;

        static concurrent_unordered_map<std::string, std::shared_ptr<MultiModelTxn>> MultiModelTxnMap;

        ///merge
        static std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
        epoch_merge_map, ///epoch merge   row_header
        local_epoch_abort_txn_set,
        epoch_abort_txn_set; /// for epoch final check

        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>>
        epoch_txn_map, epoch_write_set_map, epoch_back_txn_map;

        static concurrent_unordered_map<std::string, std::string>
            read_version_map,
                read_version_map_data, ///read validate for higher isolation
        read_version_map_csn, ///read validate for higher isolation
        insert_set;   ///插入集合，用于判断插入是否可以执行成功 check key exits?

        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
                epoch_read_validate_queue,
                epoch_merge_queue,///存放要进行merge的事务，分片
                epoch_commit_queue,
                epoch_redo_log_queue,
                epoch_result_return_queue;///存放每个epoch要进行写日志的事务，分片写日志

        static void CacheInit();
        static void EpochCacheClear(uint64_t& epoch);
    };
}



#endif //TAAS_TRANSACTION_CACHE_H
