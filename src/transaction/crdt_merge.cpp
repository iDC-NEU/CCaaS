//
// Created by 周慰星 on 11/8/22.
//

#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "transaction/crdt_merge.h"

namespace Taas {
    bool CRDTMerge::ValidateReadSet(const Context &ctx, proto::Transaction &txn) {
        ///RC & RR
        std::string key, version;
        uint64_t csn = 0;
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() != proto::OpType::Read) {
                continue;
            }
            /// indeed, we should use the csn to check the read version,
            /// but there are some bugs in updating the csn to the storage(tikv).
            if (!Merger::read_version_map_data.getValue(row.key(), version)) {
                /// should be abort, but Taas do not connect load data,
                /// so read the init snap will get empty in read_version_map
                continue;
            }
            if (version != row.data()) {
                LOG(INFO) <<"read version check failed version : " << version << ", row.data() : " << row.data();
                return false;
            }
        }
        return true;
    }

    bool CRDTMerge::ValidateWriteSet(const Context &ctx, proto::Transaction &txn) {
        auto epoch_mod = txn.commit_epoch() % ctx.kCacheMaxLength;
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        if(Merger::epoch_abort_txn_set[epoch_mod]->contain(csn_temp, csn_temp)) {
            return false;
        }
        return true;
    }

    bool CRDTMerge::MultiMasterCRDTMerge(const Context &ctx, proto::Transaction &txn) {
        auto epoch_mod = txn.commit_epoch() % ctx.kCacheMaxLength;
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        std::string csn_result;
        bool result = true;
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            if (!Merger::epoch_merge_map[epoch_mod]->insert(row.key(), csn_temp, csn_result)) {
                Merger::epoch_abort_txn_set[epoch_mod]->insert(csn_result, csn_result);
                Merger::local_epoch_abort_txn_set[epoch_mod]->insert(csn_result, csn_result);
                result = false;
            }
        }
        return result;
    }

    bool CRDTMerge::Commit(const Context &ctx, proto::Transaction &txn) {
        auto epoch_mod = txn.commit_epoch() % ctx.kCacheMaxLength;
        auto csn_temp = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
        for(auto i = 0; i < txn.row_size(); i ++) {
            const auto& row = txn.row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            else if(row.op_type() == proto::OpType::Insert) {
                Merger::epoch_insert_set[epoch_mod]->insert(row.key(), csn_temp);
                Merger::insert_set.insert(row.key(), csn_temp);
            }
            else if(row.op_type() == proto::OpType::Delete) {
                Merger::insert_set.remove(row.key(), csn_temp);
            }
            else {
                //nothing to do
            }
            Merger::read_version_map_data.insert(row.key(), row.data());
            Merger::read_version_map_csn.insert(row.key(), csn_temp);
        }
        return true;
    }
}

