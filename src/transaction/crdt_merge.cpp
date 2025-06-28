//
// Created by 周慰星 on 11/8/22.
//

#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "transaction/crdt_merge.h"
#include "transaction/transaction_cache.h"

namespace Taas {

    bool CRDTMerge::ValidateReadSet(std::shared_ptr<proto::Transaction> txn_ptr) {
        ///RC & RR & SI
        //RC do not check read data
        auto epoch_mod = txn_ptr->commit_epoch() % TaasContext::kCacheMaxLength;
        std::string version;
        uint64_t csn = 0;
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto& row = txn_ptr->row(i);
            auto key = txn_ptr->storage_type() + ":" + row.key();
            if(row.op_type() != proto::OpType::Read) {
                continue;
            }
            /// indeed, we should use the csn to check the read version,
            /// but there are some bugs in updating the csn to the storage(tikv).
            if (!TransactionCache::read_version_map.getValue(key, version)) {
                /// should be abort, but Taas do not connect load data,
                /// so read the init snap will get empty in read_version_map
                continue;
            }
            if (version != row.data()) { /// row.data == data item.version_csn
//                continue; ///only for debug
                auto csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
                TransactionCache::epoch_abort_txn_set[epoch_mod]->insert(csn_temp, csn_temp);
//                LOG(INFO) <<"Txn read version check failed";
//                LOG(INFO) <<"read version check failed version : " << version << ", row.data() : " << row.data();
                txn_ptr.reset();
                return false;
            }
        }
        txn_ptr.reset();
        return true;
    }

    bool CRDTMerge::ValidateWriteSet(std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = txn_ptr->commit_epoch() % TaasContext::kCacheMaxLength;
        auto csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        if(TransactionCache::epoch_abort_txn_set[epoch_mod]->contain(csn_temp, csn_temp)) {
            txn_ptr.reset();
            return false;
        }
        txn_ptr.reset();
        return true;
    }

    bool CRDTMerge::MultiMasterCRDTMerge(std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = txn_ptr->commit_epoch() % TaasContext::kCacheMaxLength;
        auto csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        std::string csn_result;
        bool result = true;
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto& row = txn_ptr->row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            auto key = txn_ptr->storage_type() + ":" + row.key();
            if (!TransactionCache::epoch_merge_map[epoch_mod]->insert(key, csn_temp, csn_result)) {
                TransactionCache::epoch_abort_txn_set[epoch_mod]->insert(csn_result, csn_result);
                result = false;
            }
        }
        txn_ptr.reset();
        return result;
    }

    bool CRDTMerge::Commit(std::shared_ptr<proto::Transaction> txn_ptr) {
        auto csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto& row = txn_ptr->row(i);
            auto key = txn_ptr->storage_type() + ":" + row.key();
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            else if(row.op_type() == proto::OpType::Insert) {
                TransactionCache::insert_set.insert(key, csn_temp);
            }
            else if(row.op_type() == proto::OpType::Delete) {
                TransactionCache::insert_set.remove(key, csn_temp);
            }
            else {
                //nothing to do
            }
            TransactionCache::read_version_map.insert(key, csn_temp);
        }
        txn_ptr.reset();
        return true;
    }
}

