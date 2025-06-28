//
// Created by user on 24-3-15.
//

#include "transaction/transaction_cache.h"

namespace Taas{
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
            TransactionCache::epoch_backup_txn,
            TransactionCache::epoch_insert_set,
            TransactionCache::epoch_abort_set;

    concurrent_unordered_map<std::string, std::shared_ptr<MultiModelTxn>> TransactionCache::MultiModelTxnMap;

    std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
            TransactionCache::epoch_merge_map,
            TransactionCache::local_epoch_abort_txn_set,
            TransactionCache::epoch_abort_txn_set;

    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>>
            TransactionCache::epoch_txn_map,
            TransactionCache::epoch_write_set_map,
            TransactionCache::epoch_back_txn_map;

    concurrent_unordered_map<std::string, std::string>
            TransactionCache::read_version_map,
            TransactionCache::read_version_map_data,
            TransactionCache::read_version_map_csn,
            TransactionCache::insert_set;

    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
            TransactionCache::epoch_read_validate_queue,
            TransactionCache::epoch_merge_queue,
            TransactionCache::epoch_commit_queue,
            TransactionCache::epoch_redo_log_queue,
            TransactionCache::epoch_result_return_queue;


    void TransactionCache::CacheInit() {
        auto max_length = TaasContext::kCacheMaxLength;

        ///Message handle
        epoch_backup_txn.resize(max_length);
        epoch_insert_set.resize(max_length);
        epoch_abort_set.resize(max_length);

        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_backup_txn[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_insert_set[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_abort_set[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }

        ///Merge
        epoch_merge_map.resize(max_length);
        local_epoch_abort_txn_set.resize(max_length);
        epoch_abort_txn_set.resize(max_length);
        epoch_txn_map.resize(max_length);
        epoch_back_txn_map.resize(max_length);
        epoch_write_set_map.resize(max_length);

        epoch_read_validate_queue.resize(max_length);
        epoch_merge_queue.resize(max_length);
        epoch_commit_queue.resize(max_length);
        epoch_redo_log_queue.resize(max_length);
        epoch_result_return_queue.resize(max_length);

        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_merge_map[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            local_epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            epoch_txn_map[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();
            epoch_back_txn_map[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();
            epoch_write_set_map[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();

            epoch_read_validate_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_merge_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_commit_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_result_return_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
    }

    void TransactionCache::EpochCacheClear(uint64_t &epoch) {
        auto epoch_mod_temp = epoch % TaasContext::kCacheMaxLength;

        ///Message handle
//        epoch_backup_txn[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//        epoch_insert_set[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//        epoch_abort_set[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();

        ///Merge

        epoch_merge_map[epoch_mod_temp]->clear();
        epoch_txn_map[epoch_mod_temp]->clear();
        epoch_write_set_map[epoch_mod_temp]->clear();
        epoch_back_txn_map[epoch_mod_temp]->clear();
        epoch_abort_txn_set[epoch_mod_temp]->clear();
        local_epoch_abort_txn_set[epoch_mod_temp]->clear();
    }
}