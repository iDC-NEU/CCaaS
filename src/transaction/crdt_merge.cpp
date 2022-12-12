//
// Created by 周慰星 on 11/8/22.
//
#include "transaction/crdt_merge.h"
#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {
    bool Taas::CRDTMerge::ValidateReadSet(Taas::Context &ctx, proto::Transaction &txn) {
        UNUSED_VALUE(ctx);
        return true;
        std::string key, version;
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() != proto::OpType::Read) {
                continue;
            }
            if (!EpochManager::read_version_map.getValue(row.key(), version) || version != row.data()) {
                return false;
            }
        }
        return true;
    }

    bool Taas::CRDTMerge::ValidateWriteSet(Taas::Context &ctx, proto::Transaction &txn) {
        UNUSED_VALUE(ctx);
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        if(EpochManager::current_epoch_abort_txn_set.contain(csn_temp, csn_temp)) {
            return false;
        }
        return true;
    }

    bool Taas::CRDTMerge::LocalCRDTMerge(Taas::Context &ctx, proto::Transaction &txn) {
        auto epoch_mod = txn.commit_epoch() % ctx.kCacheMaxLength;
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        std::string csn_result;
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            if (!EpochManager::epoch_merge_map[epoch_mod]->insert(row.key(), csn_temp, csn_result)) {///可以做rollback
                return false;
            }
        }
        return true;
    }

    bool Taas::CRDTMerge::MultiMasterCRDTMerge(Taas::Context &ctx, proto::Transaction &txn) {
        UNUSED_VALUE(ctx);
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        std::string csn_result;
        bool result = true;
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            if (!EpochManager::current_epoch_abort_txn_set.insert(row.key(), csn_temp, csn_result)) {
                EpochManager::current_epoch_abort_txn_set.insert(csn_result, csn_result);
                result = false;
            }
        }
        return result;
    }

    bool CRDTMerge::Commit(Context &ctx, proto::Transaction &txn) {
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            if(row.op_type() == proto::OpType::Insert) {
                EpochManager::insert_set.insert(row.key(), csn_temp);
            }
            EpochManager::read_version_map.insert(row.key(), csn_temp);
        }
        return true;
    }

    void CRDTMerge::RedoLog(Context &ctx, proto::Transaction &txn) {
        uint64_t epoch_id = txn.commit_epoch();
        auto lsn = EpochManager::epoch_log_lsn.IncCount(epoch_id, 1);
        auto key = std::to_string(epoch_id) + ":" + std::to_string(lsn);
        EpochManager::committed_txn_cache[epoch_id % EpochManager::max_length]->insert(key, txn);

    }
}

